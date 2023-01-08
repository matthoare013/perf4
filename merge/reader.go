package merge

type reader interface {
	Process(minBytes int, zero int64, arr []int)
	GetMinTs() int64
	close() error
}
