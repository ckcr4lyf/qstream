#!/usr/bin/env bash
# Run the full resiliency scenario matrix, in order, saving each summary.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=/tmp/lab
mkdir -p $OUT
for scn in baseline loss5-master loss10-all loss20-peer2 burst-master delay100-all delay300-loss5-peer1 dup-reorder-master; do
    echo; echo "########## SCENARIO $scn ##########"
    ./scripts/run_scenario.sh $scn 150
done
echo; echo "########## SCENARIO kill-peer3 ##########"
./scripts/run_scenario.sh kill-peer3 180
echo; echo "########## SCENARIO kill-master ##########"
./scripts/run_scenario.sh kill-master 150
echo; echo "ALL SCENARIOS DONE"
