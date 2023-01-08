package merge

import (
	"fmt"
	"os"
	"syscall"
)

type MMapReaderMinBytes struct {
	file *os.File
	mmap []byte
}

func NewMMapReaderMinBytes(fullFilePath string) (*MMapReaderMinBytes, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}

	size, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file size:%w", err)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return &MMapReaderMinBytes{
		file: f,
		mmap: data,
	}, nil
}

func (m *MMapReaderMinBytes) close() error {
	if err := m.file.Close(); err != nil {
		fmt.Printf("failed to close file:%v\n", err)
	}
	if err := syscall.Munmap(m.mmap); err != nil {
		fmt.Printf("failed to unmap file:%v \n", err)
	}

	return nil
}

func (r *MMapReaderMinBytes) process(minBytes int, zero int64, arr []int) {
	var (
		position = 0
	)
	//var count int
	for position < len(r.mmap) {
		//	count++
		var ts int64
		ts, position = r.readLine(position, minBytes)
		index := int32(ts - zero)
		arr[index]++
	}
	//fmt.Printf("READER COUNT -> %d \n", count)
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
	var total int64
	for i := minbytes; i < 13; i++ {
		total += int64(r.mmap[startPos+i]-'0') * pow[i]
	}

	return total, startPos + 13 + 1
}
