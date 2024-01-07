#!/bin/sh

exec cargo run --release --example create_measurements -- "$@"
