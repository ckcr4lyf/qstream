#!/usr/bin/env bash
# NAT lab: each netns is a "home router" with its own NAT rules; the
# master lives on the host at a public-ish address (10.99.0.1).
#
#   brA 10.0.0.0/24 — home1 (10.0.0.2) + home2 (10.0.0.3), same LAN
#   brB 10.1.0.0/24 — cone1 (10.1.0.2), isolated LAN
#   public IPs: master 10.99.0.1 (host lo), home1 10.99.0.2, home2 10.99.0.3,
#               cone1 10.99.0.4
#   home1/home2: SNAT (port-preserving, symmetric-ish); cone1: SNAT + DNAT
#               (full cone)
#   host routes 10.99.0.{2,3,4}/32 back into the owning netns so replies
#   to NATed peers get reverse-translated by conntrack.
#
# usage: natlab.sh {setup|teardown|start|stop|status}
set -euo pipefail
cd "$(dirname "$0")/.."

PUB="10.99.0"
DIR=/tmp/natlab

setup() {
    teardown >/dev/null 2>&1 || true
    sudo sysctl -qw net.ipv4.ip_forward=1
    sudo sysctl -qw net.ipv4.conf.all.rp_filter=0
    sudo ip addr add $PUB.1/32 dev lo 2>/dev/null || true
    sudo ip link add brA type bridge 2>/dev/null || true
    sudo ip link add brB type bridge 2>/dev/null || true
    sudo ip link set brA up; sudo ip link set brB up
    sudo ip addr add 10.0.0.1/24 dev brA 2>/dev/null || true
    sudo ip addr add 10.1.0.1/24 dev brB 2>/dev/null || true

    mkhome() { # $1=ns $2=ip $3=bridge $4=public $5=host-veth $6=gateway
        sudo ip netns add "$1"
        sudo ip link add "$5" type veth peer name "veth_$1"
        sudo ip link set "veth_$1" netns "$1"
        sudo ip link set "$5" master "$3"; sudo ip link set "$5" up
        sudo ip netns exec "$1" ip addr add "$2/24" dev "veth_$1"
        sudo ip netns exec "$1" ip link set "veth_$1" up
        sudo ip netns exec "$1" ip link set lo up
        sudo ip netns exec "$1" ip route add default via "$6"
        # Host route for replies to this home's public IP, pinned to src
        # 10.99.0.1 (the master's address — conntrack reverse-translation
        # requires the reply to come FROM it), plus a PERMANENT neighbor so
        # the NAT pseudo-address resolves without ARP (DEVLOG: ARP was
        # blackholing replies).
        local mac
        mac=$(sudo ip netns exec "$1" ip link show "veth_$1" | grep -oE 'link/ether [0-9a-f:]+' | awk '{print $2}')
        sudo ip route add "$4/32" dev "$5" src $PUB.1 2>/dev/null || true
        sudo ip neigh replace "$4" dev "$5" lladdr "$mac" nud permanent
    }
    mkhome home1 10.0.0.2 brA $PUB.2 vh1 10.0.0.1
    mkhome home2 10.0.0.3 brA $PUB.3 vh2 10.0.0.1
    mkhome cone1 10.1.0.2 brB $PUB.4 vh3 10.1.0.1

    # Host: bypass the pre-existing MASQUERADE for our public range so
    # replies to NATed peers reach them unmangled (DEVLOG). Also remove
    # leftovers from the first natlab design, if any.
    sudo iptables -t nat -D PREROUTING -d $PUB.4 -j DNAT --to-destination 10.1.0.2 2>/dev/null || true
    sudo iptables -t nat -D POSTROUTING -s 10.0.0.2 ! -d 10.0.0.0/24 -j SNAT --to-source $PUB.2 2>/dev/null || true
    sudo iptables -t nat -D POSTROUTING -s 10.0.0.3 ! -d 10.0.0.0/24 -j SNAT --to-source $PUB.3 2>/dev/null || true
    sudo iptables -t nat -D POSTROUTING -s 10.1.0.2 ! -d 10.1.0.0/24 -j SNAT --to-source $PUB.4 2>/dev/null || true
    sudo iptables -t nat -D POSTROUTING -d $PUB.0/24 -j RETURN 2>/dev/null || true
    sudo iptables -t nat -I POSTROUTING 1 -d $PUB.0/24 -j RETURN

    # NAT inside each home (per-netns tables)
    # home1/home2: SNAT to their public IP, except same-LAN (brA) traffic
    # and broadcasts (beacons stay private; two -d flags aren't allowed
    # in this iptables, so broadcasts RETURN first)
    sudo ip netns exec home1 iptables -t nat -A POSTROUTING -d 255.255.255.255 -j RETURN
    sudo ip netns exec home1 iptables -t nat -A POSTROUTING -s 10.0.0.2 ! -d 10.0.0.0/24 -j SNAT --to-source $PUB.2
    sudo ip netns exec home2 iptables -t nat -A POSTROUTING -d 255.255.255.255 -j RETURN
    sudo ip netns exec home2 iptables -t nat -A POSTROUTING -s 10.0.0.3 ! -d 10.0.0.0/24 -j SNAT --to-source $PUB.3
    # cone1: SNAT outbound + DNAT all inbound (full cone)
    sudo ip netns exec cone1 iptables -t nat -A POSTROUTING -s 10.1.0.2 -j SNAT --to-source $PUB.4
    sudo ip netns exec cone1 iptables -t nat -A PREROUTING -d $PUB.4 -j DNAT --to-destination 10.1.0.2
    echo "NAT lab setup complete"
}

teardown() {
    sudo pkill -x qstream 2>/dev/null || true
    sleep 1
    sudo iptables -t nat -D POSTROUTING -d $PUB.0/24 -j RETURN 2>/dev/null || true
    for ns in home1 home2 cone1; do
        sudo ip netns del $ns 2>/dev/null || true
    done
    for br in brA brB; do sudo ip link del $br 2>/dev/null || true; done
    for ip in 1 2 3 4; do sudo ip addr del $PUB.$ip/32 dev lo 2>/dev/null || true; done
    echo "NAT lab torn down"
}

start() {
    mkdir -p $DIR/h1 $DIR/h2 $DIR/c1
    sudo pkill -x qstream 2>/dev/null || true
    sleep 1
    ./target/release/qstream server 3333 ./live/live.m3u8 18080 > $DIR/master.log 2>&1 &
    sleep 1
    sudo ip netns exec home1 env QSTREAM_NO_UPNP=1 QSTREAM_NAME=home1 ./target/release/qstream peer 4444 $PUB.1 3333 $DIR/h1 18081 > $DIR/h1.log 2>&1 &
    sudo ip netns exec home2 env QSTREAM_NO_UPNP=1 QSTREAM_NAME=home2 ./target/release/qstream peer 4444 $PUB.1 3333 $DIR/h2 18081 > $DIR/h2.log 2>&1 &
    sudo ip netns exec cone1 env QSTREAM_NO_UPNP=1 QSTREAM_NAME=cone1 ./target/release/qstream peer 4444 $PUB.1 3333 $DIR/c1 18081 > $DIR/c1.log 2>&1 &
    sleep 2
    echo "master + 3 NATed peers started (logs in $DIR)"
}

stop() {
    sudo pkill -x qstream 2>/dev/null || true
    sudo pkill -x qstream 2>/dev/null || true
    sleep 1
    echo "stopped"
}

status() {
    echo "master:      $(curl -s --max-time 2 http://127.0.0.1:18080/health 2>/dev/null || echo down)"
    for name in h1 h2 c1; do
        echo "$name:        segs=$(ls $DIR/$name/seg_*.ts 2>/dev/null | wc -l)"
    done
    echo "--- master /peers (public view) ---"
    curl -s --max-time 2 http://127.0.0.1:18080/peers 2>/dev/null | head -7
    echo "--- home1 /peers (NATed view) ---"
    sudo ip netns exec home1 curl -s --max-time 2 http://127.0.0.1:18081/peers 2>/dev/null | head -7 || echo "(no curl inside netns)"
}

case "${1:-}" in
    setup) setup ;;
    teardown) teardown ;;
    start) start ;;
    stop) stop ;;
    status) status ;;
    *) echo "usage: natlab.sh {setup|teardown|start|stop|status}"; exit 1 ;;
esac
