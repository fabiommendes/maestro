#!/bin/sh

exec wasmtime --dir . python.wasm "$@"
