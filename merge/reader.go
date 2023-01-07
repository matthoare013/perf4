package merge

import (
	"fmt"
)

var (
	readerFunc = func(fullFilePath string) (reader, error) {
		return NewFileReader(fullFilePath)
	}
)

type File struct {
	filePath string
	reader   reader
}

func NewFile(fullFilePath string) (*File, error) {
	reader, err := readerFunc(fullFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to make reader")
	}

	file := &File{
		filePath: fullFilePath,
		reader:   reader,
	}

	return file, nil
}

func (f *File) MinTs() int64 {
	return f.reader.GetMinTs()
}

func (r *File) Close() error {
	if err := r.reader.close(); err != nil {
		fmt.Printf("failed to close reader:%v \n", err)
	}

	// todo return error
	return nil
}

type reader interface {
	process(minTs, maxTs int64, arr []int)
	GetMinTs() int64
	close() error
}
