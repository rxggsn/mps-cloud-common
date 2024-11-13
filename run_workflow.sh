#!/usr/bin/env sh

run_test() {
    manifest_path=$1
    threads=$2
    cargo test --manifest-path "$manifest_path" --features full,rocksdb-enable,diesel-enable -- --test-threads=$threads
    status_code=$?
    if [ "$status_code" = 101 ]
    then
        cargo test --manifest-path "$manifest_path" --features full,rocksdb-enable,diesel-enable -- --test-threads=$threads
        status_code=$?
        if [ "$status_code" = 101 ]
        then
            exit 101
        fi
    fi
}

cargo check --features full,rocksdb-enable,diesel-enable

run_test Cargo.toml 1