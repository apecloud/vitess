#!/bin/bash

source "$(dirname "${BASH_SOURCE[0]:-$0}")/../env-apecloud.sh"

echo "Stopping vtconsensus."
kill -9 "$(cat "$VTDATAROOT/tmp/vtconsensus.pid")"

