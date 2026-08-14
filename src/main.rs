//! qstream — P2P live video streaming over UDP. See SPEC.md.

mod log;
mod peer;
mod protocol;
mod server;

use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;

const USAGE: &str = "\
qstream — P2P live video streaming over UDP (see SPEC.md)

USAGE:
    qstream server <port>                                  Master/seed mode
    qstream peer <local-port> <remote-ip> <remote-port>    Peer mode
    qstream --help                                         Show this help

EXAMPLES:
    qstream server 3333
    qstream peer 4444 127.0.0.1 3333

Node name can be set with the QSTREAM_NAME environment variable
(defaults to \"master\" / \"peer\").
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }

    match args[0].as_str() {
        "server" => {
            let Some(port_str) = args.get(1) else {
                eprintln!("server: missing <port>");
                print!("{USAGE}");
                return ExitCode::FAILURE;
            };
            let Ok(port) = port_str.parse::<u16>() else {
                eprintln!("server: invalid port: {port_str}");
                return ExitCode::FAILURE;
            };
            let name = node_name("master");
            match server::run(port, &name) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("server error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        "peer" => {
            let Some(local_port_str) = args.get(1) else {
                eprintln!("peer: missing <local-port>");
                print!("{USAGE}");
                return ExitCode::FAILURE;
            };
            let Some(remote_ip_str) = args.get(2) else {
                eprintln!("peer: missing <remote-ip>");
                print!("{USAGE}");
                return ExitCode::FAILURE;
            };
            let Some(remote_port_str) = args.get(3) else {
                eprintln!("peer: missing <remote-port>");
                print!("{USAGE}");
                return ExitCode::FAILURE;
            };

            let Ok(local_port) = local_port_str.parse::<u16>() else {
                eprintln!("peer: invalid local port: {local_port_str}");
                return ExitCode::FAILURE;
            };
            let Ok(remote_ip) = remote_ip_str.parse::<IpAddr>() else {
                eprintln!("peer: invalid remote ip: {remote_ip_str}");
                return ExitCode::FAILURE;
            };
            let Ok(remote_port) = remote_port_str.parse::<u16>() else {
                eprintln!("peer: invalid remote port: {remote_port_str}");
                return ExitCode::FAILURE;
            };

            let remote = SocketAddr::new(remote_ip, remote_port);
            let name = node_name("peer");
            match peer::run(local_port, remote, &name) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("peer error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        other => {
            eprintln!("unknown command: {other}");
            print!("{USAGE}");
            ExitCode::FAILURE
        }
    }
}

fn node_name(default: &str) -> String {
    std::env::var("QSTREAM_NAME").unwrap_or_else(|_| default.to_string())
}
