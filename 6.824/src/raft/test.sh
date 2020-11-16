#!/bin/bash

#export GOPATH="/home/zyx/Desktop/mit6824/6.824"
#export PATH="$PATH:/usr/lib/go-1.9/bin"

rm res -rf
mkdir res

for ((i = 0; i < 10; i++))
do

    for ((c = $((i*30)); c < $(( (i+1)*30)); c++))
    do                  #replace job name here
         (go test -run TestFigure8Unreliable2C ) &> ./res/$c &
    done

    sleep 40

    grep -nr "FAIL.*raft.*" res

done