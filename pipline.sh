#!/bin/bash

set -e

mkdir -p /ClickHouse_new/build
cd /ClickHouse_new
rm -rf contrib
cp -r /root/contrib .
cmake -S . -B build
cmake --build build
objcopy --strip-debug build/programs/clickhouse
chmod 777 build/programs/clickhouse
