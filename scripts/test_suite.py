#!/usr/bin/env python3
"""Deterministic, self-contained qstream integration test suite.

The suite does not use tmux, ffmpeg, the checked-in live playlist, or a live
network. It creates a synthetic HLS origin in a temporary directory, starts
real qstream processes, verifies HTTP behavior and byte-for-byte replication,
and exercises peer sharing plus a deterministic 5% loss link.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TIMEOUT = 60.0


class SuiteError(RuntimeError):
    pass


class Process:
    def __init__(self, label: str, argv: list[str], env: dict[str, str], log_path: Path):
        self.label = label
        self.log_path = log_path
        self.log = log_path.open("wb")
        self.proc = subprocess.Popen(
            argv,
            cwd=ROOT,
            env=env,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def stop(self) -> None:
        if self.proc.poll() is not None:
            self.log.close()
            return
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
            self.proc.wait(timeout=3)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait(timeout=3)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                pass
        self.log.close()

    def assert_alive(self) -> None:
        code = self.proc.poll()
        if code is not None:
            tail = self.log_path.read_text(errors="replace")[-2000:]
            raise SuiteError(f"{self.label} exited with {code}\n{tail}")


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http_get(port: int, path: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/{path.lstrip('/')}", timeout=2
        ) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return 0, b""


def wait_for(label: str, predicate, processes: list[Process], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for process in processes:
            process.assert_alive()
        if predicate():
            return
        time.sleep(0.25)
    raise SuiteError(f"timed out waiting for {label} ({timeout:.0f}s)")


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


def make_origin(root: Path, count: int = 10, base: int = 1000) -> tuple[Path, list[str]]:
    origin = root / "origin"
    origin.mkdir(parents=True)
    names: list[str] = []
    durations = [1.0, 1.5, 2.0]
    for index in range(count):
        number = base + index
        name = f"seg_{number}.ts"
        # Deliberately vary packetization boundaries, including a zero-byte
        # segment and a segment larger than one transfer window.
        sizes = [0, 1, 1399, 1400, 1401, 4097, 12000]
        size = sizes[index % len(sizes)]
        payload = bytes(((number + offset * 17) % 251 for offset in range(size)))
        (origin / name).write_bytes(payload)
        names.append(name)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:2",
        f"#EXT-X-MEDIA-SEQUENCE:{base}",
    ]
    for index, name in enumerate(names):
        lines += [f"#EXTINF:{durations[index % len(durations)]:.1f},", name]
    (origin / "live.m3u8").write_text("\n".join(lines) + "\n")
    return origin, names


def base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "QSTREAM_LOG": "info",
            "QSTREAM_NO_UPNP": "1",
            "QSTREAM_ORIGIN_SEEDERS": "1",
            "RUST_BACKTRACE": "1",
        }
    )
    return env


def start_node(
    suite: Path,
    label: str,
    mode: str,
    udp_port: int,
    http_port: int,
    manifest: Path,
    data_dir: Path | None = None,
    bootstrap: int | None = None,
    extra_env: dict[str, str] | None = None,
) -> Process:
    binary = ROOT / "target" / "release" / "qstream"
    env = base_env()
    env["QSTREAM_NAME"] = label
    if extra_env:
        env.update(extra_env)
    if mode == "server":
        argv = [str(binary), "server", str(udp_port), str(manifest), str(http_port)]
    else:
        if data_dir is None or bootstrap is None:
            raise ValueError("peer requires data_dir and bootstrap")
        argv = [
            str(binary),
            "peer",
            str(udp_port),
            "127.0.0.1",
            str(bootstrap),
            str(data_dir),
            str(http_port),
        ]
    return Process(label, argv, env, suite / f"{label}.log")


def compare_segments(origin: Path, data_dir: Path, names: list[str]) -> None:
    missing = [name for name in names if not (data_dir / name).is_file()]
    if missing:
        raise SuiteError(f"{data_dir.name}: missing segments: {missing}")
    mismatched = [
        name
        for name in names
        if file_hash(origin / name) != file_hash(data_dir / name)
    ]
    if mismatched:
        raise SuiteError(f"{data_dir.name}: hash mismatch: {mismatched}")


def run_basic_and_sharing(suite: Path) -> dict[str, object]:
    root = suite / "sharing"
    origin, names = make_origin(root, count=10, base=1000)
    master_udp, master_http = reserve_port(), reserve_tcp_port()
    p1_udp, p1_http = reserve_port(), reserve_tcp_port()
    p2_udp, p2_http = reserve_port(), reserve_tcp_port()
    processes: list[Process] = []
    try:
        master = start_node(suite, "sharing-master", "server", master_udp, master_http, origin / "live.m3u8")
        processes.append(master)
        wait_for("master health", lambda: http_get(master_http, "health")[0] == 200, processes, 10)

        p1_dir = root / "peer1"
        p1 = start_node(suite, "sharing-peer1", "peer", p1_udp, p1_http, origin / "live.m3u8", p1_dir, master_udp)
        processes.append(p1)
        wait_for("peer 1 replication", lambda: all((p1_dir / name).is_file() for name in names), processes, DEFAULT_TIMEOUT)
        compare_segments(origin, p1_dir, names)
        wait_for("peer 1 playback", lambda: http_get(p1_http, "playback.m3u8")[0] == 200, processes, 10)

        status, body = http_get(p1_http, "playback.m3u8")
        if status != 200 or b"#EXTM3U" not in body:
            raise SuiteError("peer 1 playback playlist is not valid HLS")
        status, _ = http_get(p1_http, "%2e%2e/live.m3u8")
        if status != 404:
            raise SuiteError(f"path traversal returned HTTP {status}, expected 404")

        # Wait until the master's view confirms peer 1 has a fresh path and
        # inventory. This makes the origin-seeder assertion deterministic:
        # peer 2 must use peer 1 instead of receiving a recovery seed.
        def peer1_ready() -> bool:
            _, peers = http_get(master_http, "peers")
            text = peers.decode(errors="replace")
            return "sharing-peer1" in text and "path=fresh" in text and "newest=" in text

        wait_for("peer 1 fresh inventory", peer1_ready, processes, 25)

        p2_dir = root / "peer2"
        p2 = start_node(suite, "sharing-peer2", "peer", p2_udp, p2_http, origin / "live.m3u8", p2_dir, master_udp)
        processes.append(p2)
        wait_for("peer 2 replication", lambda: all((p2_dir / name).is_file() for name in names), processes, DEFAULT_TIMEOUT)
        compare_segments(origin, p2_dir, names)
        log = (suite / "sharing-peer2.log").read_text(errors="replace")
        if f"from 127.0.0.1:{p1_udp}" not in log:
            raise SuiteError("peer 2 completed without a pull from peer 1")
        return {"segments": len(names), "peer1_udp": p1_udp, "peer2_udp": p2_udp}
    finally:
        for process in reversed(processes):
            process.stop()


def run_loss_case(suite: Path) -> dict[str, object]:
    root = suite / "loss"
    origin, names = make_origin(root, count=7, base=2000)
    master_udp, master_http = reserve_port(), reserve_tcp_port()
    peer_udp, peer_http = reserve_port(), reserve_tcp_port()
    processes: list[Process] = []
    try:
        master = start_node(
            suite,
            "loss-master",
            "server",
            master_udp,
            master_http,
            origin / "live.m3u8",
            extra_env={
                "QSTREAM_FAULT_DROP_PCT": "5",
                "QSTREAM_FAULT_SEED": "424242",
            },
        )
        processes.append(master)
        wait_for("loss master health", lambda: http_get(master_http, "health")[0] == 200, processes, 10)
        peer_dir = root / "peer"
        peer = start_node(suite, "loss-peer", "peer", peer_udp, peer_http, origin / "live.m3u8", peer_dir, master_udp)
        processes.append(peer)
        wait_for("loss replication", lambda: all((peer_dir / name).is_file() for name in names), processes, 75)
        compare_segments(origin, peer_dir, names)
        log = (suite / "loss-master.log").read_text(errors="replace")
        if "fault injection:" not in log or "drop 5%" not in log:
            raise SuiteError("loss case did not enable the deterministic fault injector")
        return {"segments": len(names), "master_udp": master_udp}
    finally:
        for process in reversed(processes):
            process.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true", help="keep the temporary suite directory")
    args = parser.parse_args()
    if not (ROOT / "target" / "release" / "qstream").is_file():
        raise SuiteError("target/release/qstream is missing; run cargo build --release first")

    suite = Path(tempfile.mkdtemp(prefix="qstream-test-suite-"))
    print(f"suite directory: {suite}")
    try:
        sharing = run_basic_and_sharing(suite)
        print(f"PASS basic HTTP, byte integrity, playback, traversal, peer sharing: {sharing['segments']} segments")
        loss = run_loss_case(suite)
        print(f"PASS deterministic 5% loss recovery and byte integrity: {loss['segments']} segments")
        print("PASS qstream deterministic integration suite")
        if args.keep:
            print(f"kept: {suite}")
        else:
            shutil.rmtree(suite)
        return 0
    except Exception as error:
        print(f"FAIL {error}", file=sys.stderr)
        print(f"artifacts kept at: {suite}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
