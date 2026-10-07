#!/bin/sh
# Developer-only: emit the optimized release machine code of the market-data library and
# print the disassembly of selected functions, so hot paths can be checked for bounds
# checks, allocation calls, missed inlining or missed vectorization.
#
#   scripts/inspect-asm.sh [FUNCTION_SUBSTRING ...]
#
# Supports native x86-64 ELF objects; cross targets via CARGO_BUILD_TARGET or Cargo config
# build.target are unsupported. Output goes to ${CARGO_TARGET_DIR:-target}/asm-audit/:
# the full .s listing and the object
# disassembly. Nothing here changes how the crate is built for use; it is the same release
# profile, for the default (portable) target CPU. Machine code stays compiler generated.
set -eu
cd "$(dirname "$0")/.."
if [ -n "${CARGO_BUILD_TARGET:-}" ]; then
  echo "inspect-asm supports the native target only; unset CARGO_BUILD_TARGET" >&2
  exit 1
fi
target=${CARGO_TARGET_DIR:-target}
export CARGO_TARGET_DIR="$target"
out=$target/asm-audit
mkdir -p "$out"
trap 'rm -f "$out/fn.tmp" "$out/calls.tmp" "$out/calls.sorted"' EXIT
cargo rustc --release --lib --locked -q -- --emit=asm,obj -C debuginfo=1
lib_s=
for candidate in "$target"/release/deps/market_data-*.s; do
  [ -f "$candidate" ] || continue
  if [ -z "$lib_s" ] || [ "$candidate" -nt "$lib_s" ]; then lib_s=$candidate; fi
done
if [ -z "$lib_s" ] || [ ! -f "${lib_s%.s}.o" ]; then
  echo "native assembly/object artifacts not found under $target/release/deps" >&2
  exit 1
fi
lib_o=${lib_s%.s}.o
format=$(llvm-objdump -f "$lib_o")
case "$format" in
  *"file format elf64-x86-64"*) ;;
  *) echo "inspect-asm relocation counts support x86-64 ELF only" >&2; exit 1 ;;
esac
cp "$lib_s" "$out/market_data.s"
llvm-objdump -d -r --demangle --no-show-raw-insn "$lib_o" > "$out/market_data.dis"
echo "full listing: $out/market_data.s, disassembly: $out/market_data.dis"
[ "$#" -eq 0 ] && set -- "<market_data::features::book_features>" "<<market_data::book::OrderBook>::apply>" \
  "<<market_data::features::TradeFlow>::advance>" "<<market_data::book::DepthSync>::handle>" \
  "<<market_data::pipeline::Pipeline>::handle>"
for name in "$@"; do
  # One function block: from its symbol line to the next blank line.
  awk -v n="$name" '/^[0-9a-f]+ <.*>:$/ { show = index($0, n) > 0 } show { print } /^$/ { show = 0 }' \
    "$out/market_data.dis" > "$out/fn.tmp"
  if [ ! -s "$out/fn.tmp" ]; then
    echo "no function matched selector: $name" >&2
    exit 1
  fi
  lines=$(grep -cE '^[[:space:]]+[[:xdigit:]]+:[[:space:]]+[a-z][a-z0-9.]*([[:space:]]|$)' "$out/fn.tmp" || true)
  awk '/R_X86_64_(PLT32|GOTPCREL)/ { sub(/.*R_X86_64_[A-Z0-9]+[[:space:]]+/, ""); sub(/-0x4$/, ""); calls[$0]++ }
       END { for (call in calls) printf "   %d %s\n", calls[call], call }' "$out/fn.tmp" > "$out/calls.tmp"
  sort -rn "$out/calls.tmp" > "$out/calls.sorted"
  echo "== $name: $lines instructions"
  echo "   bounds-check panics: $(grep -cE 'R_X86_64.*panic_bounds_check' "$out/fn.tmp" || true), alloc calls: $(grep -cE 'R_X86_64.*(__rust_alloc|__rust_realloc)' "$out/fn.tmp" || true), packed SIMD ops: $(grep -cE '\b(v?p(add|sub|mul)[bwdq]|v?(add|mul)p[sd])\b' "$out/fn.tmp" || true)"
  head -12 "$out/calls.sorted"
done
