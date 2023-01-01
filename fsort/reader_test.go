package fsort

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadLine(t *testing.T) {
	tests := []struct {
		name          string
		mmap          []byte
		startPosition int

		expectedTs     int64
		expectedOffset int
	}{
		{
			name:           "basic",
			mmap:           []byte{'1', '2', '3', '\n'},
			startPosition:  0,
			expectedTs:     123,
			expectedOffset: 4,
		},
		{
			name:           "basic",
			mmap:           []byte{'1', '2', '3', '\n', '4', '5', '6', '\n'},
			startPosition:  4,
			expectedTs:     456,
			expectedOffset: 8,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := &Reader{
				mmap: test.mmap,
			}

			actualTs, actualOffSet := r.readLine(test.startPosition)

			require.Equal(t, test.expectedTs, actualTs)
			require.Equal(t, test.expectedOffset, actualOffSet)
		})
	}
}

func TestReadFile(t *testing.T) {
	reader, err := NewReader("../files/t.txt")
	require.NoError(t, err)

	data := reader.dataProcessing(reader.GetMinTs())
	for d := range data {
		fmt.Println(d)
	}
}
