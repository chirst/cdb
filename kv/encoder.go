package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func Encode(v []interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&v)
	if err != nil {
		return nil, fmt.Errorf("err encoding value %w", err)
	}
	return buf.Bytes(), nil
}

func Decode(v []byte) ([]any, error) {
	buf := bytes.NewBuffer(v)
	var s []any
	err := gob.NewDecoder(buf).Decode(&s)
	if err != nil {
		return nil, fmt.Errorf("err decoding value %w", err)
	}
	return s, nil
}

func EncodeKey(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(&v)
	if err != nil {
		return nil, fmt.Errorf("err encoding key %w", err)
	}
	return buf.Bytes(), nil
}

func DecodeKey(v []byte) (any, error) {
	buf := bytes.NewBuffer(v)
	var s any
	err := gob.NewDecoder(buf).Decode(&s)
	if err != nil {
		return nil, fmt.Errorf("err decoding key %w", err)
	}
	return s, nil
}
