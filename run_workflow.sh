#!/usr/bin/env sh

run_test() {
    manifest_path=$1
    threads=$2
    cargo test --manifest-path "$manifest_path" --all-features -- --test-threads=$threads
    return $?
}

cargo check

run_test common/Cargo.toml 4
status_code=$?
if [ "$status_code" == 101 ]
then
    run_test common/Cargo.toml 4
    status_code=$?
    if [ "$status_code" == 101 ]
    then
        exit 101
    fi
fi
