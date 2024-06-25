package kv

import (
	"reflect"
	"testing"
)

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
