package main

import (
	"matthoare013/sort/merge"
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
	merge, err := merge.NewMergeSort(files, outputFile)
	if err != nil {
		panic(err)
	}

	if err := merge.Merge(); err != nil {
		panic(err)
	}

	if err := merge.Close(); err != nil {
		panic(err)
	}
}
