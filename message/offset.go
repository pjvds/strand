package message

import "fmt"

type Offset uint64

func (this Offset) GoString() string {
	return fmt.Sprint(this)
}

const EmptyOffset Offset = 0

func (this Offset) Next() Offset {
	return Offset(this + 1)
}

func (this Offset) Add(delta Offset) Offset {
	return Offset(this + delta)
}

func (this Offset) Sub(delta Offset) Offset {
	return Offset(this - delta)
}

func (this Offset) AddInt(delta int) Offset {
	return Offset(uint64(this) + uint64(delta))
}
