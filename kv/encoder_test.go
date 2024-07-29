package kv

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestEncoding(t *testing.T) {
	t.Run("encode/decode", func(t *testing.T) {
		v := []any{"table", "foo", "foo", 1, "{columns:[{name:\"first_name\",type:\"TEXT\"}]}"}
		vb, err := Encode(v)
		if err != nil {
			t.Fatalf("expected no err got err: %s", err)
		}
		dv, err := Decode(vb)
		if err != nil {
			t.Fatalf("expected no err got err: %s", err)
		}
		if !reflect.DeepEqual(v, dv) {
			t.Fatalf("expected %v to be %v", v, dv)
		}
	})

	t.Run("encode/decode key", func(t *testing.T) {
		v := 1
		vb, err := EncodeKey(v)
		if err != nil {
			t.Fatal(err)
		}
		dv, err := DecodeKey(vb)
		if err != nil {
			t.Fatal(err)
		}
		if dv != v {
			t.Fatalf("expected %d got %d", v, dv)
		}
	})

	t.Run("compare encoded key", func(t *testing.T) {
		for i := 0; i < math.MaxInt16; i += 1 {
			k1, err := EncodeKey(i)
			if err != nil {
				t.Fatal(err)
			}
			k2, err := EncodeKey(i + 1)
			if err != nil {
				t.Fatal(err)
			}
			c := bytes.Compare(k1, k2)
			if c != -1 {
				t.Fatal("expected k1 to be less than k2")
			}
		}
	})
}
