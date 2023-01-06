package sort

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"golang.org/x/sync/semaphore"
)

var (
	byteArray = make([]byte, 0, 20)
)

type Merge struct {
	readers []*Reader
}

func NewMergeSort(fullFilePaths []string) (*Merge, error) {
	if len(fullFilePaths) < 1 {
		return nil, errors.New("at least 1 file required")
	}

	var readers []*Reader
	for _, f := range fullFilePaths {
		r, err := NewReader(f)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}

	return &Merge{
		readers: readers,
	}, nil
}

func (m *Merge) Merge(outputFile string) error {
	min, max, err := m.minMax()
	if err != nil {
		return err
	}

	arr := make([]int32, max-min+1)

	wg := sync.WaitGroup{}
	buffer := make(chan []int, 5)

	a := sync.WaitGroup{}
	a.Add(1)
	go func() {
		defer a.Done()
		for i := range buffer {
			for _, j := range i {
				arr[j]++
			}
		}
	}()

	sem := semaphore.NewWeighted(5)
	for _, r := range m.readers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.TODO(), 1); err != nil {
				panic(err)
			}
			defer sem.Release(1)
			s := 5_000
			buff := make([]int, 0, s)
			count := 0
			for i := range r.data {
				count++
				index := m.getIndex(min, i)
				buff = append(buff, index)
				if count == s-10 {
					count = 0
					buffer <- buff
					buff = make([]int, 0, s)
				}
			}
			buffer <- buff
		}()
	}
	wg.Wait()
	close(buffer)
	a.Wait()

	if err := m.writeResults(outputFile, arr, min); err != nil {
		return err
	}

	return nil
}

func (m *Merge) writeResults(fileName string, arr []int32, min int64) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	f.Close()

	f, err = os.OpenFile(fileName, os.O_RDWR, 0755)
	if err != nil {
		return nil
	}
	mmap, _ := mmap.Map(f, mmap.RDWR, 0)

	w := bufio.NewWriter(bytes.NewBuffer(mmap))
	for index, i := range arr {
		if i == 0 {
			continue
		}

		intToByte(min + int64(index))
		for j := int32(0); j < i; j++ {
			if _, err := w.Write(byteArray); err != nil {
				return err
			}
		}
	}

	pan(w.Flush())
	pan(mmap.Flush())
	pan(mmap.Unmap())
	pan(f.Close())

	return nil
}

func pan(err error) {
	if err != nil {
		panic(err)
	}
}

func intToByte(a int64) {
	byteArray = byteArray[:0]
	for a != 0 {
		d := a % 10
		byteArray = append(byteArray, byte(int64('0')+d))
		a = a / 10
	}
	for i, j := 0, len(byteArray)-1; i < j; i, j = i+1, j-1 {
		byteArray[i], byteArray[j] = byteArray[j], byteArray[i]
	}
	byteArray = append(byteArray, '\n')
}

func (m *Merge) getIndex(min, ts int64) int {
	return int(ts - min)
}

func (m *Merge) minMax() (int64, int64, error) {
	min, err := m.getMinTs()
	if err != nil {
		return 0, 0, err
	}
	max := time.UnixMilli(min).Add(time.Hour * 24)

	return min, max.UnixMilli(), nil
}

func (m *Merge) getMinTs() (int64, error) {
	min, err := m.readers[0].GetMinTs()
	if err != nil {
		return 0, err
	}

	for _, r := range m.readers {
		m, err := r.GetMinTs()
		if err != nil {
			return 0, err
		}
		if m < min {
			min = m
		}
	}

	return min, nil
}

func (m *Merge) Close() error {
	for _, r := range m.readers {
		if err := r.Close(); err != nil {
			fmt.Printf("failed to close file: %v \n", err)
		}
	}

	return nil
}
