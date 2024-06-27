package kv

import (
	"bytes"
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

func EncodeKey(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeKey(v []byte) (any, error) {
	buf := bytes.NewBuffer(v)
	var s any
	err := gob.NewDecoder(buf).Decode(&s)
	if err != nil {
		return nil, err
	}
	return s, nil
}
