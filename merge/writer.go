package merge

import (
	"bufio"
	"os"
)

type BasicFileWriter struct {
	file *os.File
}

func NewBasicFileWriter(resultFilePath string) *BasicFileWriter {
	f, err := os.OpenFile(resultFilePath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil
	}

	return &BasicFileWriter{
		file: f,
	}
}

func (b *BasicFileWriter) write(arr []int, min int64) error {
	w := bufio.NewWriterSize(b.file, 4096*20)

	minByte := IntToByte(min)

	for _, i := range arr {
		if i != 0 {
			for j := 0; j < i; j++ {
				if _, err := w.Write(minByte); err != nil {
					return err
				}
			}
		}
		AddToByte(1, minByte)
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}

func (f *BasicFileWriter) close() error {
	return f.file.Close()
}
