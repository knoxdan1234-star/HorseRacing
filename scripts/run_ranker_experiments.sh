#!/usr/bin/env bash
# Sequentially run three ranker tuning experiments and rename outputs.
set -e

cd "$(dirname "$0")/.."
source venv/bin/activate
export DYLD_FALLBACK_LIBRARY_PATH=/Users/kafaisie/opt/anaconda3/lib

REPORT_DIR="output/backtests"
DEFAULT_REPORT="${REPORT_DIR}/backtest_2023-09-01_2026-04-01.json"

run_exp () {
  local name="$1"; shift
  local logfile="backtest_ranker_${name}.log"
  echo "=== $(date) START $name ==="
  python scripts/run_backtest.py \
    --start 2023-09-01 --end 2026-04-01 \
    --model lightgbm --model-kind ranker \
    "$@" \
    > "$logfile" 2>&1
  if [ -f "$DEFAULT_REPORT" ]; then
    mv "$DEFAULT_REPORT" "${REPORT_DIR}/backtest_ranker_${name}.json"
  fi
  echo "=== $(date) END $name ==="
}

# Exp 1: lower edge gate (5%) + smaller Kelly (1%)
run_exp "exp1_edge05_kelly01" --edge 0.05 --kelly 0.01

# Exp 2: no edge gate + 1% bankroll cap
run_exp "exp2_edge0_cap01" --edge 0.0 --kelly 0.05 --max-bet-pct 0.01

# Exp 3: high min odds (5.0), default ranker params otherwise
run_exp "exp3_minodds5" --min-odds 5.0

echo "=== ALL EXPERIMENTS DONE: $(date) ==="
