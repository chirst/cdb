package kv

import (
	"bytes"
	"log"
	"testing"
)

func mustNewKv() *KV {
	kv, err := New(true, "")
	if err != nil {
		log.Fatal(err)
	}
	return kv
}

func mustNewCursor(root int) (*KV, *Cursor) {
	kv, err := New(true, "")
	if err != nil {
		log.Fatal(err)
	}
	return kv, kv.NewCursor(root)
}

func TestGet(t *testing.T) {
	k := []byte{1}
	v := []byte{'n', 'e', 'd'}
	_, cursor := mustNewCursor(1)
	cursor.Set(k, v)
	res, found := cursor.Get(k)
	if !found {
		t.Errorf("expected value for %v to be found", k)
	}
	if !bytes.Equal(res, v) {
		t.Errorf("expected value %v got %v", v, res)
	}
}

func TestSetPageSplit(t *testing.T) {
	kv, cursor := mustNewCursor(1)
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
			t.Fatal(err)
		}
		v := []byte{1, 0, 0, 0}
		cursor.Set(k, v)
		if ri == i {
			rk = k
			rv = v
		}
		kv.EndWriteTransaction()
	}
	res, found := cursor.Get(rk)
	if !found {
		t.Fatalf("expected value for %v to be found", rk)
	}
	if !bytes.Equal(rv, res) {
		t.Errorf("expected value %v got %v", rv, res)
	}
}

func TestBulkInsertAndGet(t *testing.T) {
	kv, cursor := mustNewCursor(1)

	// bulk insert
	amount := 500_000
	kv.BeginWriteTransaction()
	for i := 1; i <= amount; i += 1 {
		k, err := EncodeKey(i)
		if err != nil {
			t.Fatal(err)
		}
		v, err := Encode([]any{i})
		if err != nil {
			t.Fatal(err)
		}
		cursor.Set(k, v)
	}
	kv.EndWriteTransaction()

	// get middle
	midProbe := amount / 2
	mpk, err := EncodeKey(midProbe)
	if err != nil {
		t.Fatal(err)
	}
	mr, _ := cursor.Get(mpk)
	mrv, err := Decode(mr)
	if err != nil {
		t.Fatal(err)
	}
	mrvi := mrv[0].(int)
	if mrvi != midProbe {
		t.Fatalf("want mid to be %d got %d", midProbe, mrv)
	}

	// get left (min)
	leftProbe := 1
	lpk, err := EncodeKey(leftProbe)
	if err != nil {
		t.Fatal(err)
	}
	lr, _ := cursor.Get(lpk)
	lrv, err := Decode(lr)
	if err != nil {
		t.Fatal(err)
	}
	lrvi := lrv[0].(int)
	if lrvi != leftProbe {
		t.Fatalf("want left to be %d got %d", leftProbe, lrv)
	}

	// get right (max)
	rightProbe := amount
	rpk, err := EncodeKey(rightProbe)
	if err != nil {
		t.Fatal(err)
	}
	rr, _ := cursor.Get(rpk)
	rrv, err := Decode(rr)
	if err != nil {
		t.Fatal(err)
	}
	rrvi := rrv[0].(int)
	if rrvi != rightProbe {
		t.Fatalf("want right to be %d got %d", rightProbe, rrv)
	}
}

func TestUpdateLoop(t *testing.T) {
	kv := mustNewKv()

	// Seed values 1, 2, 3.
	c := kv.NewCursor(2)
	kv.BeginWriteTransaction()
	for i := range 3 {
		k, err := EncodeKey(i + 1)
		if err != nil {
			t.Fatalf("failed encoding key %s", err)
		}
		v, err := Encode([]any{i + 1})
		if err != nil {
			t.Fatalf("failed encoding value %s", err)
		}
		c.Set(k, v)
	}
	kv.EndWriteTransaction()

	// Increment values by one with the cursor.
	c = kv.NewCursor(2)
	kv.BeginWriteTransaction()
	c.GotoFirstRecord()
	for i := range 3 {
		k, err := EncodeKey(i + 1)
		if err != nil {
			t.Fatalf("failed encoding key %s", err)
		}
		v, err := Encode([]any{i + 2})
		if err != nil {
			t.Fatalf("failed encoding value %s", err)
		}
		c.DeleteCurrent()
		c.Set(k, v)
		c.GotoNext()
	}
	kv.EndWriteTransaction()

	// Check values
	c = kv.NewCursor(2)
	kv.BeginReadTransaction()
	c.GotoFirstRecord()
	for i := range 3 {
		v, err := Decode(c.GetValue())
		if err != nil {
			t.Fatalf("failed to decode value %s", err)
		}
		vi, ok := v[0].(int)
		if !ok {
			t.Fatal("vi is not int")
		}
		if vi != i+2 {
			t.Fatalf("vi should be %d but got %d", i+2, vi)
		}
		c.GotoNext()
	}
	kv.EndReadTransaction()
}

func TestUpdateLoopWithIf(t *testing.T) {
	kv := mustNewKv()

	// Seed values 1, 2, 3.
	c := kv.NewCursor(2)
	kv.BeginWriteTransaction()
	for i := range 3 {
		k, err := EncodeKey(i + 1)
		if err != nil {
			t.Fatalf("failed encoding key %s", err)
		}
		v, err := Encode([]any{i + 1})
		if err != nil {
			t.Fatalf("failed encoding value %s", err)
		}
		c.Set(k, v)
	}
	kv.EndWriteTransaction()

	// Increment values by one except for second value with the cursor.
	c = kv.NewCursor(2)
	kv.BeginWriteTransaction()
	c.GotoFirstRecord()
	for i := range 3 {
		k, err := EncodeKey(i + 1)
		if err != nil {
			t.Fatalf("failed encoding key %s", err)
		}
		v, err := Encode([]any{i + 2})
		if err != nil {
			t.Fatalf("failed encoding value %s", err)
		}
		if i != 2 {
			c.DeleteCurrent()
			c.Set(k, v)
		}
		c.GotoNext()
	}
	kv.EndWriteTransaction()

	// Check values
	c = kv.NewCursor(2)
	kv.BeginReadTransaction()
	c.GotoFirstRecord()
	for i := range 3 {
		v, err := Decode(c.GetValue())
		if err != nil {
			t.Fatalf("failed to decode value %s", err)
		}
		vi, ok := v[0].(int)
		if !ok {
			t.Fatal("vi is not int")
		}
		if i == 2 {
			if vi != i+1 {
				t.Fatalf("vi should be %d but got %d", i+1, vi)
			}
		} else {
			if vi != i+2 {
				t.Fatalf("vi should be %d but got %d", i+2, vi)
			}
		}
		c.GotoNext()
	}
	kv.EndReadTransaction()
}
