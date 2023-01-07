package merge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMergeSort(t *testing.T) {
	type args struct {
		fullFilePaths  []string
		resultFilePath string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "basic",
			args: args{
				fullFilePaths:  []string{"../files/t.txt"},
				resultFilePath: "../result.txt",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merge, err := NewMergeSort(tt.args.fullFilePaths, tt.args.resultFilePath)
			require.NoError(t, err)

			require.NoError(t, merge.Merge())
		})
	}
}
