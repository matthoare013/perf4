package merge

import "fmt"

// byte array assume to have \n at the end
func AddToByte(a int, byteArray []byte) {
	var carry int
	pos := len(byteArray) - 2
	f := func(b int) {
		n := int(byteArray[pos]) - '0' + b
		if n >= 10 {
			n = n - 10
			carry = 1
		} else {
			carry = 0
		}
		byteArray[pos] = byte(int64(n) + '0')
	}

	f(a)
	for carry == 1 {
		pos--
		f(1)
	}
}

// returns byte array with \n
func IntToByte(a int64) []byte {
	byteArray := make([]byte, 0, 14)
	byteArray = byteArray[:0]
	for a != 0 {
		d := a % 10
		byteArray = append(byteArray, byte(int64('0')+d))
		a = a / 10
	}
	for i, j := 0, len(byteArray)-1; i < j; i, j = i+1, j-1 {
		byteArray[i], byteArray[j] = byteArray[j], byteArray[i]
	}
	byteArray = append(byteArray, '\n')
	return byteArray
}

func MaxBytesToRead(minTs, maxTs int64) int {
	return len(fmt.Sprintf("%d", maxTs-minTs))
}
