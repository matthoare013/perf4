package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	now := time.Now()
	max := now.Add(24 * time.Hour)

	f, err := os.OpenFile("./40m_g.txt", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}

	format := func() int64 {
		return now.UnixMilli()
	}

	rand.Seed(time.Now().UnixNano())
	w := bufio.NewWriterSize(f, 4096*20)
	for i := 0; i < 40_000_000; i++ {
		if rand.Intn(2) == 1 {
			if !now.After(max) {
				now = now.Add(1 * time.Millisecond)
			}
		}
		_, err := w.WriteString(fmt.Sprintf("%d\n", format()))
		if err != nil {
			panic(err)
		}
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}

	if err := f.Close(); err != nil {
		panic(err)
	}
}
