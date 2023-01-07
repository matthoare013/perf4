#!/bin/bash

rm -rf ./pprof/
mkdir ./pprof/

rm -f result.txt

go build main.go
mv main mittins
rm -rf result.txt

./exec.sh ./mittins files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt files/40m.txt

wc -l result.txt
cmp -b result.txt files/big.txt