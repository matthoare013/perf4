package merge

import (
	"fmt"
	"sync"
	"testing"
)

func TestPutIndex(t *testing.T) {
	l, c := makeRobin()
	fmt.Println(l)

	m := &Merge{
		roundRobin: l,
		robinChan:  c,
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100_000; i++ {
				index := m.getIndex()
				m.putBackIndex(index)
			}
		}()
	}
	wg.Wait()
}

func TestMerge_getIndex(t *testing.T) {
	type fields struct {
		readers    []reader
		writer     writer
		roundRobin int
		robinChan  chan int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Merge{
				readers:    tt.fields.readers,
				writer:     tt.fields.writer,
				roundRobin: tt.fields.roundRobin,
				robinChan:  tt.fields.robinChan,
			}
			if got := m.getIndex(); got != tt.want {
				t.Errorf("Merge.getIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
