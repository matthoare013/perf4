package merge

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	write = func(resultFilePath string) (writer, error) {
		return NewBasicFileWriter(resultFilePath), nil
	}
)

type Merge struct {
	readers    []reader
	writer     writer
	roundRobin int

	robinChan chan int
}

func NewMergeSort(fullFilePaths []string, resultFilePath string) (*Merge, error) {
	if len(fullFilePaths) < 1 {
		return nil, errors.New("at least 1 file required")
	}

	var readers []reader
	for _, f := range fullFilePaths {
		r, err := NewMMapReaderMinBytes(f)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}

	writer, err := write(resultFilePath)
	if err != nil {
		return nil, err
	}

	l, c := makeRobin()

	return &Merge{
		readers:    readers,
		writer:     writer,
		roundRobin: l,
		robinChan:  c,
	}, nil
}

func makeRobin() (int, chan int) {
	l := runtime.NumCPU() + 1
	robinChan := make(chan int, l)
	for i := 0; i < l; i++ {
		robinChan <- i
	}
	return l, robinChan
}

func (m *Merge) Merge() error {
	min, max, err := m.minMax()
	if err != nil {
		return err
	}

	minBytes := BytesToSkip(min, max)
	zero := FindNewZero(min, minBytes)

	arr := make([]int, max-min+1)
	data := make([][]int, m.roundRobin)
	for i := 0; i < m.roundRobin; i++ {
		data[i] = make([]int, max-min+1)
	}

	wg := sync.WaitGroup{}
	sem := semaphore.NewWeighted(int64(m.roundRobin))
	for _, r := range m.readers {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.TODO(), 1); err != nil {
				panic(err)
			}
			defer sem.Release(1)

			index := m.getIndex()
			defer m.putBackIndex(index)
			r.Process(minBytes, zero, data[index])
			r.close()
		}()
	}
	wg.Wait()

	m.sumArrays(data, arr, 4)

	if err := m.writer.write(arr, min); err != nil {
		return err
	}

	return nil
}

func (m *Merge) getIndex() int {
	return <-m.robinChan
}

func (m *Merge) putBackIndex(index int) {
	m.robinChan <- index
}

func (m *Merge) Close() error {
	if err := m.writer.close(); err != nil {
		fmt.Printf("failed to writer reader:%v\n", err)
	}

	return nil
}

func (m *Merge) sumArrays(data [][]int, arr []int, split int) {
	wg := sync.WaitGroup{}
	rSplit := len(arr) / split
	for i := 0; i < split; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := rSplit * i
			end := start + rSplit
			if i == split-1 {
				end = len(arr)
			}

			for i := start; i < end; i++ {
				var sum int
				for j := range data {
					sum += data[j][i]
				}
				arr[i] = sum
			}
		}()
	}
	wg.Wait()
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
	min := m.readers[0].GetMinTs()

	for _, r := range m.readers {
		m := r.GetMinTs()
		if m < min {
			min = m
		}
	}

	return min, nil
}
