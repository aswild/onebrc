#!/bin/bash

cargo build --release

for measurements in testdata/measurements-*.txt; do
    rm -f t.out
    if ! cargo run --release -q -- $measurements >"t.out"; then
        echo "$measurements: FAIL TO RUN"
    elif diff ${measurements%.txt}.out t.out &>/dev/null; then
        echo "$measurements: PASS"
    else
        echo "$measurements: FAIL"
        diff -u ${measurements%.txt}.out t.out
    fi
done
rm -f t.out
