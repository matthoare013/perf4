package fsort

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkReadLine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		r := &Reader{}
		var a []byte
		for j := 0; j < 13; j++ {
			a = append(a, '1')
		}
		a = append(a, '\n')
		r.mmap = a
		b.StartTimer()

		_, _ = r.readLine(0)
	}
}

func TestReadLine(t *testing.T) {
	now := time.Now().UnixMilli()
	input := fmt.Sprintf(`%d\n%d\n`, now, now)
	tests := []struct {
		name          string
		mmap          []byte
		startPosition int

		expectedTs     int64
		expectedOffset int
	}{
		// {
		// 	name:           "basic",
		// 	mmap:           []byte(input),
		// 	startPosition:  0,
		// 	expectedTs:     now,
		// 	expectedOffset: 14,
		// },
		{
			name:           "basic",
			mmap:           []byte(input),
			startPosition:  15,
			expectedTs:     now,
			expectedOffset: 14 + 14 + 1,
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
		//fmt.Println(d)
		_ = d
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
