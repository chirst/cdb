package kv

import (
	"bytes"
	"testing"
)

func TestGet(t *testing.T) {
	kv, _ := New(true, "")
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
}

func TestSetPageSplit(t *testing.T) {
	kv, _ := New(true, "")
	var rk []byte
	var rv []byte
	ri := 178
	// For a page 4096 a split is more than guaranteed here because 512*8=4096
	// not including the header of each page.
	iters := 4096 / 8
	for i := 1; i <= iters; i += 1 {
		kv.BeginWriteTransaction()
		k, err := EncodeKey(i)
		if err != nil {
			t.Fatal(err.Error())
		}
		v := []byte{1, 0, 0, 0}
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
}

func TestBulkInsertAndGet(t *testing.T) {
	kv, _ := New(true, "")

	// bulk insert
	amount := 500_000
	kv.BeginWriteTransaction()
	for i := 1; i <= amount; i += 1 {
		k, err := EncodeKey(i)
		if err != nil {
			t.Fatal(err.Error())
		}
		v, err := Encode([]any{i})
		if err != nil {
			t.Fatal(err.Error())
		}
		err = kv.Set(1, k, v)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
	kv.EndWriteTransaction()

	// get middle
	midProbe := amount / 2
	mpk, err := EncodeKey(midProbe)
	if err != nil {
		t.Fatal(err.Error())
	}
	mr, _, err := kv.Get(1, mpk)
	if err != nil {
		t.Fatal(err.Error())
	}
	mrv, err := Decode(mr)
	if err != nil {
		t.Fatal(err.Error())
	}
	mrvi := mrv[0].(int)
	if mrvi != midProbe {
		t.Fatalf("want mid to be %d got %d", midProbe, mrv)
	}

	// get left (min)
	leftProbe := 1
	lpk, err := EncodeKey(leftProbe)
	if err != nil {
		t.Fatal(err.Error())
	}
	lr, _, err := kv.Get(1, lpk)
	if err != nil {
		t.Fatal(err.Error())
	}
	lrv, err := Decode(lr)
	if err != nil {
		t.Fatal(err.Error())
	}
	lrvi := lrv[0].(int)
	if lrvi != leftProbe {
		t.Fatalf("want left to be %d got %d", leftProbe, lrv)
	}

	// get right (max)
	rightProbe := amount
	rpk, err := EncodeKey(rightProbe)
	if err != nil {
		t.Fatal(err.Error())
	}
	rr, _, err := kv.Get(1, rpk)
	if err != nil {
		t.Fatal(err.Error())
	}
	rrv, err := Decode(rr)
	if err != nil {
		t.Fatal(err.Error())
	}
	rrvi := rrv[0].(int)
	if rrvi != rightProbe {
		t.Fatalf("want right to be %d got %d", rightProbe, rrv)
	}
}
