package merge

import (
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
)

type MMapReaderMinBytes struct {
	file *os.File
	mmap mmap.MMap
}

var _ reader = (*MMapReader)(nil)

func NewMMapReaderMinBytes(fullFilePath string) (*MMapReaderMinBytes, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}

	return &MMapReaderMinBytes{
		file: f,
		mmap: mmap,
	}, nil
}

func (m *MMapReaderMinBytes) close() error {
	if err := m.file.Close(); err != nil {
		fmt.Printf("failed to close file:%v\n", err)
	}
	if err := m.mmap.Unmap(); err != nil {
		fmt.Printf("failed to unmap file:%v\n", err)
	}

	return nil
}

func (r *MMapReaderMinBytes) process(minTs, maxTs int64, arr []int) {
	minBytes := BytesToSkip(minTs, maxTs)
	zero := FindNewZero(minTs, minBytes)

	var (
		position = 0
	)
	for position < len(r.mmap) {
		var ts int64
		ts, position = r.readLine(position, minBytes)
		index := int32(ts - zero)
		arr[index]++
	}
}

// GetMinTs implements reader
func (r *MMapReaderMinBytes) GetMinTs() int64 {
	ts, _ := r.readLine(0, 0)
	return ts
}

var pow = []int64{
	1000000000000,
	100000000000,
	10000000000,
	1000000000,
	100000000,
	10000000,
	1000000,
	100000,
	10000,
	1000,
	100,
	10,
	1,
}

func (r *MMapReaderMinBytes) readLine(startPos, minbytes int) (int64, int) {
	f := func(p int) int64 {
		return int64(r.mmap[p] - '0')
	}

	var total int64
	for i := minbytes; i < 13; i++ {
		total += int64(f(startPos+i)) * pow[i]
	}

	return total, startPos + 13 + 1
}
