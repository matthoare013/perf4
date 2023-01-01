package fsort

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type Reader struct {
	filePath string
	file     *os.File
	mmap     mmap.MMap
}

func NewReader(fullFilePath string) (*Reader, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}

	mmap, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		filePath: fullFilePath,
		file:     f,
		mmap:     mmap,
	}

	return reader, nil
}

func (r *Reader) dataProcessing(minTs, maxTs int64) []int {
	arr := make([]int, maxTs-minTs+1)

	var (
		position = 0
	)
	for position < len(r.mmap) {
		var ts int64
		ts, position = r.readLine(position)
		index := int32(ts - minTs)
		arr[index]++
	}

	return arr
}

func (r *Reader) GetMinTs() int64 {
	ts, _ := r.readLine(0)
	return ts
}

func (r *Reader) readLine(startPos int) (int64, int) {
	newLine := true

	var n int64
	b := r.mmap[startPos]
	var i int
	for newLine {
		if b == '\n' {
			newLine = false
			i++
			continue
		} else {
			m := int64(b) - '0'
			n = (n * 10) + int64(m)
			i++
			b = r.mmap[i+startPos]
		}
	}
	return n, i + startPos
}

func (r *Reader) Close() error {
	return r.file.Close()
}
