package merge

import (
	"os"
	"testing"

	"github.com/edsrzf/mmap-go"
)

func TestMMapReaderMinBytes_readLine(t *testing.T) {
	type fields struct {
		file *os.File
		mmap mmap.MMap
	}
	type args struct {
		startPos int
		minbytes int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		number int64
		offset int
	}{
		{
			name: "no bytes off",
			fields: fields{
				mmap: []byte(`1671669869739\n`),
			},
			args: args{
				startPos: 0,
				minbytes: 0,
			},
			number: 1671669869739,
			offset: 14,
		},
		{
			name: "4 bytes need",
			fields: fields{
				mmap: []byte(`1671669405056\n`),
			},
			args: args{
				startPos: 0,
				minbytes: 4,
			},
			number: 669405056,
			offset: 14,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &MMapReaderMinBytes{
				file: tt.fields.file,
				mmap: tt.fields.mmap,
			}
			got, got1 := r.readLine(tt.args.startPos, tt.args.minbytes)
			if got != tt.number {
				t.Errorf("MMapReaderMinBytes.readLine() got = %v, want %v", got, tt.number)
			}
			if got1 != tt.offset {
				t.Errorf("MMapReaderMinBytes.readLine() got1 = %v, want %v", got1, tt.offset)
			}
		})
	}
}
