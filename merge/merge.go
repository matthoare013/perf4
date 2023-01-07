package merge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-collections/collections/stack"
	"golang.org/x/sync/semaphore"
)

var (
	write = func(resultFilePath string) (writer, error) {
		return NewBasicFileWriter(resultFilePath), nil
	}
)

type Merge struct {
	readers []*File
	writer  writer
}

func NewMergeSort(fullFilePaths []string, resultFilePath string) (*Merge, error) {
	if len(fullFilePaths) < 1 {
		return nil, errors.New("at least 1 file required")
	}

	var readers []*File
	for _, f := range fullFilePaths {
		r, err := NewFile(f)
		if err != nil {
			return nil, err
		}
		readers = append(readers, r)
	}

	writer, err := write(resultFilePath)
	if err != nil {
		return nil, err
	}

	return &Merge{
		readers: readers,
		writer:  writer,
	}, nil
}

func (m *Merge) Merge() error {
	min, max, err := m.minMax()
	if err != nil {
		return err
	}

	minBytes := BytesToSkip(min, max)
	zero := FindNewZero(min, minBytes)

	stack := stack.New()
	arr := make([]int, max-min+1)
	arrayLen := 5
	data := make([][]int, arrayLen)
	for i := 0; i < arrayLen; i++ {
		data[i] = make([]int, max-min+1)
		stack.Push(i % arrayLen)
	}

	wg := sync.WaitGroup{}
	sem := semaphore.NewWeighted(int64(arrayLen))
	mu := sync.Mutex{}
	for i, r := range m.readers {
		i := i
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.TODO(), 1); err != nil {
				panic(err)
			}
			mu.Lock()
			index := stack.Pop().(int)
			mu.Unlock()

			defer sem.Release(1)
			defer func() {
				mu.Lock()
				stack.Push(i % arrayLen)
				mu.Unlock()
			}()

			r.reader.process(minBytes, zero, data[index])
		}()
	}
	wg.Wait()

	m.sumArrays(data, arr, 4)

	if err := m.writer.write(arr, min); err != nil {
		return err
	}

	return nil
}

func (m *Merge) Close() error {
	for _, r := range m.readers {
		if err := r.Close(); err != nil {
			fmt.Printf("failed to close reader:%v\n", err)
		}
	}
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
	min := m.readers[0].MinTs()

	for _, r := range m.readers {
		m := r.MinTs()
		if m < min {
			min = m
		}
	}

	return min, nil
}

type writer interface {
	write(arr []int, min int64) error
	close() error
}
