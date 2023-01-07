package merge

import (
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
)

type MMapReader struct {
	file *os.File
	mmap mmap.MMap
}

var _ reader = (*MMapReader)(nil)

func NewFileReader(fullFilePath string) (*MMapReader, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}

	return &MMapReader{
		file: f,
		mmap: mmap,
	}, nil
}

func (m *MMapReader) close() error {
	if err := m.file.Close(); err != nil {
		fmt.Printf("failed to close file:%v\n", err)
	}
	if err := m.mmap.Unmap(); err != nil {
		fmt.Printf("failed to unmap file:%v\n", err)
	}

	return nil
}

func (r *MMapReader) process(minTs, maxTs int64, arr []int) {
	var (
		position = 0
	)
	for position < len(r.mmap) {
		var ts int64
		ts, position = r.readLine(position)
		index := int32(ts - minTs)
		arr[index]++
	}
}

// GetMinTs implements reader
func (r *MMapReader) GetMinTs() int64 {
	ts, _ := r.readLine(0)
	return ts
}

func (r *MMapReader) readLine(startPos int) (int64, int) {
	f := func(p int) int64 {
		return int64(r.mmap[p] - '0')
	}

	n := f(startPos)*1000000000000 +
		f(startPos+1)*100000000000 +
		f(startPos+2)*10000000000 +
		f(startPos+3)*1000000000 +
		f(startPos+4)*100000000 +
		f(startPos+5)*10000000 +
		f(startPos+6)*1000000 +
		f(startPos+7)*100000 +
		f(startPos+8)*10000 +
		f(startPos+9)*1000 +
		f(startPos+10)*100 +
		f(startPos+11)*10 +
		f(startPos+12)

	return n, startPos + 13 + 1
}
