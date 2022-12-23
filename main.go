package main

import (
	"matthoare013/sort/sort"

	"os"
)

const (
	//outputFile = "/mnt/ssd/out/matty_result.txt"
	outputFile = "result.txt"
)

func main() {
	//defer profile.Start(profile.ProfilePath("./pprof/"), profile.CPUProfile).Stop()
	//debug.SetGCPercent(-1)

	args := os.Args
	if len(args) < 1 {
		panic("no input files")
	}

	files := os.Args[1:]
	merge, err := sort.NewMergeSort(files)
	if err != nil {
		panic(err)
	}

	if err := merge.Merge(outputFile); err != nil {
		panic(err)
	}

	if err := merge.Close(); err != nil {
		panic(err)
	}
}
