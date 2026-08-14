#!/usr/bin/env python3
"""Analyze a lab scenario directory -> metrics summary.

Usage: metrics.py /tmp/lab/<scenario>
Reads node logs + the master's live/ segments and prints a summary.
"""
import glob
import os
import re
import statistics
import sys

DIR = sys.argv[1] if len(sys.argv) > 1 else "/tmp/lab/baseline"
MASTER_LIVE = "/home/ubuntu/Documents/qstream/live"

TS = re.compile(r"^\[(\d+)\] (\w+)  (.*)$")
PULL = re.compile(r"pulling (\S+) from (\S+) \(transfer")
DLOAD = re.compile(r"downloaded (\S+) \((\d+) bytes, (\d+) packets, (\d+)ms, (\d+) KB/s\)")
FAIL = re.compile(r"pull failed: (.*)$")
RANK = re.compile(r"peer ranking: (.*)$")
SERVE = re.compile(r"serving (\S+) to (\S+) \(transfer")


def parse_log(path):
    pulls, downloads, fails, evicts, ranks, manifest_msgs = [], [], [], [], [], []
    served_by = {}
    for line in open(path, errors="replace"):
        m = TS.match(line)
        if not m:
            continue
        ts, level, msg = int(m.group(1)), m.group(2), m.group(3)
        if (p := PULL.search(msg)):
            pulls.append((ts, p.group(1), p.group(2)))
        elif (d := DLOAD.search(msg)):
            downloads.append((ts, d.group(1), int(d.group(2)), int(d.group(3)), int(d.group(4)), int(d.group(5))))
        elif (f := FAIL.search(msg)):
            fails.append((ts, f.group(1)))
        elif "evicting" in msg:
            evicts.append((ts, msg))
        elif (r := RANK.search(msg)):
            ranks.append((ts, r.group(1)))
        elif (s := SERVE.search(msg)):
            served_by[s.group(2)] = served_by.get(s.group(2), 0) + 1
        if "manifest updated" in msg:
            manifest_msgs.append(ts)
    return {"pulls": pulls, "downloads": downloads, "fails": fails,
            "evicts": evicts, "ranks": ranks, "served_by": served_by,
            "manifests": manifest_msgs}


def classify_fail(msg):
    if "does not have" in msg:
        return "not_found"
    if "no response" in msg:
        return "timeout"
    if "incomplete" in msg:
        return "incomplete"
    return "other"


def master_seg_mtimes(start_ts):
    """Segment creation times from the end-of-run snapshot (epoch ms)."""
    mtimes = {}
    path = os.path.join(DIR, "master_segs.txt")
    if not os.path.exists(path):
        return mtimes
    for line in open(path):
        parts = line.split()
        if len(parts) < 6:
            continue
        name = os.path.basename(parts[-1])
        m = re.match(r"seg_(\d+)\.ts", name)
        if m and parts[-2].isdigit():
            mt = int(parts[-2]) * 1000
            if mt >= start_ts:
                mtimes[int(m.group(1))] = mt
    return mtimes


def success_by_source(data):
    """For each saved segment, the source of the LAST pull attempt before
    its download — that's the peer that actually served it."""
    by_file = {}
    for ts, name, src in data["pulls"]:
        by_file[name] = (ts, src)
    counts = {}
    for ts, name, *_ in data["downloads"]:
        if name in by_file:
            src = by_file[name][1]
            counts[src] = counts.get(src, 0) + 1
    return counts


def lag_stats(downloads, mtimes, start_ts):
    """Replication lag: peer download time - master file mtime (ms)."""
    lags = []
    for ts, name, *_ in downloads:
        m = re.match(r"seg_(\d+)\.ts", name)
        if not m:
            continue
        mt = mtimes.get(int(m.group(1)))
        if mt and mt >= start_ts:
            lags.append(ts - mt)
    if not lags:
        return None
    lags.sort()
    p = lambda q: lags[min(len(lags) - 1, int(len(lags) * q))]
    return {"n": len(lags), "median": p(0.5), "p90": p(0.9), "max": lags[-1]}


def end_coverage(peer_dir):
    """How many of the segments in the master's final playlist the peer has."""
    pl = os.path.join(DIR, "master_playlist.m3u8")
    if not os.path.exists(pl):
        return None, None
    cur = [l.strip() for l in open(pl) if l.strip() and not l.startswith("#")]
    if not cur:
        return None, None
    have = set(os.path.basename(p) for p in glob.glob(os.path.join(peer_dir, "seg_*.ts")))
    return sum(1 for s in cur if s in have), len(cur)


def main():
    start_ts = 0
    mlog = os.path.join(DIR, "master.log")
    if os.path.exists(mlog):
        first = next(iter(open(mlog)), "")
        m = re.match(r"\[(\d+)\]", first)
        if m:
            start_ts = int(m.group(1))
    mtimes = master_seg_mtimes(start_ts)

    master = parse_log(os.path.join(DIR, "master.log")) if os.path.exists(os.path.join(DIR, "master.log")) else None
    if master:
        served_total = sum(master["served_by"].values())
        print(f"MASTER: served {served_total} segments; sender-fail events: {len(master['fails'])}")
        nf = sum(1 for _, m in master["fails"] if "does not have" in m)
        print(f"        pull-fail log lines: {len(master['fails'])} (mostly peers probing)")

    print("\nPEER  saved  pulls  src-dist  nf  to  inc  other  evicts  KB/s(med)  xfer_ms(med)  lag_ms(med/p90/max)  end-cov")
    for i in range(1, 6):
        path = os.path.join(DIR, f"p{i}.log")
        if not os.path.exists(path):
            continue
        data = parse_log(path)
        saved = len(data["downloads"])
        pulls = len(data["pulls"])
        ok_src = success_by_source(data)
        ok_dist = " ".join(f"{k.split(':')[1]}:{v}" for k, v in sorted(ok_src.items(), key=lambda x: -x[1])[:4])
        src = {}
        for _, _, s in data["pulls"]:
            src[s] = src.get(s, 0) + 1
        src_dist = " ".join(f"{k.split(':')[1]}:{v}" for k, v in sorted(src.items(), key=lambda x: -x[1])[:4])
        nf = to = inc = other = 0
        for _, m in data["fails"]:
            c = classify_fail(m)
            nf += c == "not_found"; to += c == "timeout"; inc += c == "incomplete"; other += c == "other"
        evicts = len(data["evicts"])
        kbs = [d[5] for d in data["downloads"]]
        xms = [d[4] for d in data["downloads"]]
        lag = lag_stats(data["downloads"], mtimes, start_ts)
        cov, covn = end_coverage(os.path.join(DIR, f"p{i}"))
        lag_s = f"{lag['median']}/{lag['p90']}/{lag['max']}" if lag else "-"
        kb_s = str(int(statistics.median(kbs))) if kbs else "-"
        xm_s = str(int(statistics.median(xms))) if xms else "-"
        cov_s = f"{cov}/{covn}" if cov is not None else "-"
        print(f"p{i}     {saved:>3}   {pulls:>4}  {src_dist:<28} {nf:>2}  {to:>2}  {inc:>3}  {other:>3}  {evicts:>3}  {kb_s:>7}  {xm_s:>9}  {lag_s:<19}  {cov_s}")
        print(f"        ok-by-src: {ok_dist}")
        if data["ranks"]:
            print(f"        last ranking: {data['ranks'][-1][1]}")

    # integrity: compare any segment present in both master and peer
    mismatches = 0
    for i in range(1, 6):
        for p in glob.glob(os.path.join(DIR, f"p{i}", "seg_*.ts")):
            name = os.path.basename(p)
            mpath = os.path.join(MASTER_LIVE, name)
            if os.path.exists(mpath) and os.path.getsize(p) != os.path.getsize(mpath):
                mismatches += 1
                print(f"  ! size mismatch p{i}: {name}")
    print(f"\nintegrity: {mismatches} size mismatches across peers")


if __name__ == "__main__":
    main()
