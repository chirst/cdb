package main

import (
	"bytes"
	"reflect"
	"testing"
)

func TestKv(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		kv, _ := NewKv(true)
		k := []byte{1}
		v := []byte{'n', 'e', 'd'}
		kv.Set(1, k, v)
		res, found, err := kv.Get(1, k)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Errorf("expected value for %v to be found", k)
		}
		if !bytes.Equal(res, v) {
			t.Errorf("expected value %v got %v", v, res)
		}
	})

	t.Run("set page split", func(t *testing.T) {
		kv, _ := NewKv(true)
		for i := 0; i < 256; i += 1 {
			kv.Set(1, []byte{byte(i)}, []byte{1, 2, 3, 4, 5})
		}
		for i := 0; i < 138; i += 1 {
			kv.Set(1, []byte{byte(i), 0}, []byte{1, 2, 3, 4, 5})
		}
		// this value causes a split
		k := []byte{byte(140), 0}
		v := []byte{1, 1, 1, 1, 1}
		kv.Set(1, k, v)
		res, found, err := kv.Get(1, k)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Errorf("expected value for %v to be found", k)
		}
		if !bytes.Equal(res, v) {
			t.Errorf("expected value %v got %v", v, res)
		}
	})
}

func TestEncoding(t *testing.T) {
	t.Run("encode/decode", func(t *testing.T) {
		v := []any{"table", "foo", "foo", 1, "{columns:[{name:\"first_name\",type:\"TEXT\"}]}"}
		vb, err := Encode(v)
		if err != nil {
			t.Fatalf("expected no err got err: %s", err.Error())
		}
		dv, err := Decode(vb)
		if err != nil {
			t.Fatalf("expected no err got err: %s", err.Error())
		}
		if !reflect.DeepEqual(v, dv) {
			t.Fatalf("expected %v to be %v", v, dv)
		}
	})
}
