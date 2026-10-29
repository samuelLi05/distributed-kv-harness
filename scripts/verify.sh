#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --locked
python3 scripts/smoke.py --binary target/debug/distributed-kv-harness
python3 scripts/smoke.py --binary target/debug/distributed-kv-harness --protocol abd_basic
python3 scripts/smoke.py --binary target/debug/distributed-kv-harness --protocol abd

python3 scripts/smoke.py --binary target/debug/distributed-kv-harness --protocol gus
