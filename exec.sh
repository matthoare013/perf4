#!/bin/bash

echo "Executing: $@"

sudo sync
sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

/usr/bin/time --format 'real: %e, user: %U, sys: %S' taskset -c 0,1,2,3 "$@"
