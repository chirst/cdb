package main

import (
	"bytes"
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
		var rk []byte
		var rv []byte
		ri := 178
		// For a page 4096 a split is more than guaranteed here because
		// 512*8=4096 not including the header of each page.
		for i := 1; i < 512; i += 1 {
			kv.BeginWriteTransaction()
			k := EncodeKey(uint16(i))
			v := []byte{1, 0, 0, 0}
			if len(k) != 4 {
				t.Fatal("need k to be len 4")
			}
			if len(v) != 4 {
				t.Fatal("need v to be len 4")
			}
			kv.Set(1, k, v)
			if ri == i {
				rk = k
				rv = v
			}
			kv.EndWriteTransaction()
		}
		res, found, err := kv.Get(1, rk)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("expected value for %v to be found", rk)
		}
		if !bytes.Equal(rv, res) {
			t.Errorf("expected value %v got %v", rv, res)
		}
	})
}
