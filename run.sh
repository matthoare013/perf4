#!/bin/bash

rm -rf ./pprof/
mkdir ./pprof/

rm result.txt

go build main.go
mv main mittins
rm -rf result.txt

./exec.sh ./mittins files/2m.txt files/4m.txt files/8m.txt files/10m.txt files/20m.txt files/40m.txt

wc -l result.txt
cmp -b result.txt files/result.txt