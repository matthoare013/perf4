package merge

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIntToByte(t *testing.T) {
	require.Equal(t, []byte{'1', '6', '7', '1', '6', '7', '0', '1', '7', '1', '2', '3', '6', '\n'}, IntToByte(1671670171236))
}

func TestAddToByte(t *testing.T) {
	t.Run("no carry", func(t *testing.T) {
		a := []byte{'1', '6', '7', '1', '6', '7', '0', '1', '7', '1', '2', '3', '6', '\n'}
		AddToByte(
			1,
			a,
		)

		require.Equal(
			t,
			[]byte{'1', '6', '7', '1', '6', '7', '0', '1', '7', '1', '2', '3', '7', '\n'},
			a,
		)
	})

	t.Run("carry", func(t *testing.T) {
		a := []byte{'1', '6', '7', '1', '6', '7', '0', '1', '7', '1', '2', '3', '9', '\n'}
		AddToByte(
			1,
			a,
		)

		require.Equal(
			t,
			[]byte{'1', '6', '7', '1', '6', '7', '0', '1', '7', '1', '2', '4', '0', '\n'},
			a,
		)
	})
}

func TestMaxBytesToRead(t *testing.T) {
	min := int64(1671670171236)
	max := time.UnixMilli(min).Add(time.Hour * 24).UnixMilli()

	fmt.Println(min, max)

	length := MaxBytesToRead(min, max)
	require.Equal(t, 8, length)
}
