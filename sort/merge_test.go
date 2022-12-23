package sort

import (
	"testing"
)

func BenchmarkIntToByte(b *testing.B) {
	for i := 0; i < b.N; i++ {
		intToByte(int64(i))
	}
}
