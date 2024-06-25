package kv

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
)

func Encode(v []interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(v []byte) ([]any, error) {
	buf := bytes.NewBuffer(v)
	var s []any
	err := gob.NewDecoder(buf).Decode(&s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func EncodeKey(v uint16) []byte {
	bp3 := make([]byte, 4)
	binary.LittleEndian.PutUint16(bp3, v)
	return bp3
}

func DecodeKey(v []byte) int {
	return int(binary.LittleEndian.Uint16(v))
}
