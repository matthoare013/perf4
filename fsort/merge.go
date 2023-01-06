package fsort

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/golang-collections/collections/stack"
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

	var totalSize int64
	for _, r := range m.readers {
		totalSize += r.fileLength()
	}

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

			r.dataProcessing(min, max, data[index])
		}()
	}
	wg.Wait()

	m.sumArrays(data, arr, 4)

	if err := m.writeResults(outputFile, arr, min, totalSize); err != nil {
		return err
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

func (m *Merge) writeResults(fileName string, arr []int, min int64, totalSize int64) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}
	pan(f.Truncate(totalSize))
	f.Close()

	f, err = os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return nil
	}
	mmap, err := mmap.Map(f, mmap.RDWR, 0)
	pan(err)

	w := bufio.NewWriterSize(f, 4096*20)
	intToByte(min)

	for _, i := range arr {
		if i == 0 {
			addToByte(1)
			continue
		}

		for j := 0; j < i; j++ {
			if _, err := w.Write(byteArray); err != nil {
				return err
			}
		}
		addToByte(1)
	}

	pan(w.Flush())
	pan(mmap.Unmap())
	pan(f.Close())

	return nil
}

func pan(err error) {
	if err != nil {
		panic(err)
	}
}

func addToByte(a int) {
	var carry int
	pos := len(byteArray) - 2
	f := func(b int) {
		n := int(byteArray[pos]) - '0' + b
		if n >= 10 {
			n = n - 10
			carry = 1
		} else {
			carry = 0
		}
		byteArray[pos] = byte(int64(n) + '0')
	}

	f(a)
	for carry == 1 {
		pos--
		f(1)
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

func (m *Merge) Close() error {
	for _, r := range m.readers {
		if err := r.Close(); err != nil {
			fmt.Printf("failed to close file: %v \n", err)
		}
	}

	return nil
}
