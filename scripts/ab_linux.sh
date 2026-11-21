#!/bin/bash
# A/B timing inside Linux: ab_linux.sh DATA RUNS BIN... (alternates binaries;
# prints min and median wall ms for each, plus mean user+sys CPU seconds).
DATA=$1; RUNS=$2; shift 2
OUT=$(mktemp -d); SCR=$(mktemp -d)
declare -A T C
for r in $(seq "$RUNS"); do
  for b in "$@"; do
    rm -rf "$OUT"/*
    s=$(date +%s%N)
    NCPU=8 MEM_GB=24 DATA_DIR="$DATA" OUTPUT_DIR="$OUT" SCRATCH_DIR="$SCR" \
      /usr/bin/time -f "%U %S" -o "$SCR/t" "$b" 2>/dev/null
    e=$(date +%s%N)
    T[$b]="${T[$b]} $(( (e - s) / 1000000 ))"
    C[$b]="${C[$b]} $(awk '{print $1 + $2}' "$SCR/t")"
  done
done
for b in "$@"; do
  w=$(echo ${T[$b]} | tr ' ' '\n' | sort -n | awk '{a[NR]=$1} END {printf "min %d  median %d", a[1], a[int((NR+1)/2)]}')
  c=$(echo ${C[$b]} | tr ' ' '\n' | awk '{s+=$1} END {printf "%.1f", s/NR}')
  echo "$b: wall ms $w | cpu s $c"
done
rm -rf "$OUT" "$SCR"
