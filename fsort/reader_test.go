package fsort

import (
	"fmt"
	"testing"
	"time"

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

	min := reader.GetMinTs()
	max := time.UnixMilli(min).Add(time.Hour * 24)

	data := reader.dataProcessing(min, max.UnixMilli())
	for d := range data {
		fmt.Println(d)
	}
}

func TestAddToByte(t *testing.T) {
	tests := []struct {
		input    []byte
		n        int
		expected []byte
	}{
		{
			input:    []byte{'1', '2'},
			n:        1,
			expected: []byte{'1', '3'},
		},
		{
			input:    []byte{'1', '2'},
			n:        9,
			expected: []byte{'2', '1'},
		},
		{
			input:    []byte{'1', '9', '9'},
			n:        1,
			expected: []byte{'2', '0', '0'},
		},
	}

	for _, test := range tests {
		t.Run(string(test.input), func(t *testing.T) {
			byteArray = test.input
			addToByte(test.n)
			fmt.Println(string(byteArray))
			require.Equal(t, test.expected, byteArray)
		})
	}
}
