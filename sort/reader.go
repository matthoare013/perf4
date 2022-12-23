package sort

import (
	"bufio"
	"os"
	"strconv"

	"github.com/edsrzf/mmap-go"
)

type Reader struct {
	filePath string
	file     *os.File

	data chan int64
}

func NewReader(fullFilePath string) (*Reader, error) {
	f, err := os.Open(fullFilePath)
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		filePath: fullFilePath,
		file:     f,
	}

	data := reader.startDataProcessing()
	reader.data = data

	return reader, nil
}

func (r *Reader) startDataProcessing() chan int64 {
	var (
		data = make(chan int64, 500)
	)

	go func() {
		defer close(data)

		mmap, err := mmap.Map(r.file, mmap.RDONLY, 0)
		if err != nil {
			panic(err)
		}

		var n int64
		for _, b := range mmap {
			if b == '\n' {
				data <- n
				n = 0
			} else {
				m := int64(b) - '0'
				n = (n * 10) + int64(m)
			}
		}
	}()

	return data
}

func (r *Reader) GetMinTs() (int64, error) {
	readFile, err := os.Open(r.filePath)
	if err != nil {
		return 0, err
	}
	defer readFile.Close()

	scanner := bufio.NewScanner(readFile)
	scanner.Scan()
	s := scanner.Text()
	x, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	return x, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}
