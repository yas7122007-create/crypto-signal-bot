#!/bin/sh
# Developer-only: emit the optimized release machine code of the market-data library and
# print the disassembly of selected functions, so hot paths can be checked for bounds
# checks, allocation calls, missed inlining or missed vectorization.
#
#   scripts/inspect-asm.sh [FUNCTION_SUBSTRING ...]
#
# Output goes to target/asm-audit/ (ignored by git): the full .s listing and the object
# disassembly. Nothing here changes how the crate is built for use; it is the same release
# profile, for the default (portable) target CPU. Machine code stays compiler generated.
set -eu
cd "$(dirname "$0")/.."
out=target/asm-audit
mkdir -p "$out"
trap 'rm -f "$out/fn.tmp"' EXIT
cargo rustc --release --lib --locked -q -- --emit=asm,obj -C debuginfo=1
lib_s=$(ls -t target/release/deps/market_data-*.s | head -1)
lib_o=$(ls -t target/release/deps/market_data-*.o | head -1)
cp "$lib_s" "$out/market_data.s"
llvm-objdump -d -r --demangle --no-show-raw-insn "$lib_o" > "$out/market_data.dis"
echo "full listing: $out/market_data.s, disassembly: $out/market_data.dis"
[ "$#" -eq 0 ] && set -- "<market_data::features::book_features>" "<<market_data::book::OrderBook>::apply>" \
  "<<market_data::features::TradeFlow>::advance>" "<<market_data::book::DepthSync>::handle>" \
  "<<market_data::pipeline::Pipeline>::handle>"
for name in "$@"; do
  # One function block: from its symbol line to the next blank line.
  awk -v n="$name" '/^[0-9a-f]+ <.*>:$/ { show = index($0, n ":") > 0 } show { print } /^$/ { show = 0 }' \
    "$out/market_data.dis" > "$out/fn.tmp"
  lines=$(grep -c '^ ' "$out/fn.tmp" || true)
  calls=$(grep -E 'R_X86_64_(PLT32|GOTPCREL)' "$out/fn.tmp" | sed -E 's/.*R_X86_64_[A-Z0-9]+[[:space:]]+//; s/-0x4$//' | sort | uniq -c | sort -rn | head -12)
  echo "== $name: $lines instructions"
  echo "   bounds-check panics: $(grep -cE 'R_X86_64.*panic_bounds_check' "$out/fn.tmp" || true), alloc calls: $(grep -cE 'R_X86_64.*(__rust_alloc|__rust_realloc)' "$out/fn.tmp" || true), packed SIMD ops: $(grep -cE '\b(v?p(add|sub|mul)[bwdq]|v?(add|mul)p[sd])\b' "$out/fn.tmp" || true)"
  echo "$calls" | sed 's/^/   /'
done
rm -f "$out/fn.tmp"
