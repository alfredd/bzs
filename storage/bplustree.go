package storage

type ValueType uint8

/*const (
	Integer ValueType = 0
	Double  ValueType = 1
	Str     ValueType = 2

)*/

type Node struct {
	Key   string
	Value struct {
		value string
		vType ValueType
	}
	next           *Node
	prev           *Node
	readTimeStamp  int64
	writeTimeStamp int64
}
