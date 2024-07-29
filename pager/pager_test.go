package pager

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPageHelpers(t *testing.T) {
	pager, err := New(true, "")
	if err != nil {
		t.Fatal(err)
	}
	p := pager.GetPage(1)

	t.Run("get set internal", func(t *testing.T) {
		p.SetType(pageTypeInternal)
		if res := p.GetType(); res != pageTypeInternal {
			t.Errorf("want %d got %d", pageTypeInternal, res)
		}
	})

	t.Run("get set leaf", func(t *testing.T) {
		p.SetType(pageTypeLeaf)
		if res := p.GetType(); res != pageTypeLeaf {
			t.Errorf("want %d got %d", pageTypeLeaf, res)
		}
	})

	t.Run("get set record count", func(t *testing.T) {
		want := 2
		p.setRecordCount(want)
		if res := p.GetRecordCount(); res != want {
			t.Errorf("want %d got %d", want, res)
		}
	})

	t.Run("get page number", func(t *testing.T) {
		want := 1
		res := p.GetNumber()
		if res != want {
			t.Errorf("want %d got %d", want, res)
		}
	})

	t.Run("get page number as bytes", func(t *testing.T) {
		var want uint16 = 1
		res := p.GetNumberAsBytes()
		ExpectUint16(t, res, 0, want)
	})

	t.Run("get set parent page number", func(t *testing.T) {
		wantPn := 12
		p.SetParentPageNumber(wantPn)
		gotHas, gotPn := p.GetParentPageNumber()
		if gotHas != true {
			t.Error("want true got false")
		}
		if gotPn != wantPn {
			t.Errorf("got %d want %d", gotPn, wantPn)
		}
	})

	t.Run("get set left page number", func(t *testing.T) {
		wantPn := 21
		p.SetLeftPageNumber(wantPn)
		gotHas, gotPn := p.GetLeftPageNumber()
		if gotHas != true {
			t.Error("want true got false")
		}
		if gotPn != wantPn {
			t.Errorf("got %d want %d", gotPn, wantPn)
		}
	})

	t.Run("get set right page number", func(t *testing.T) {
		wantPn := 33
		p.SetRightPageNumber(wantPn)
		gotHas, gotPn := p.GetRightPageNumber()
		if gotHas != true {
			t.Error("want true got false")
		}
		if gotPn != wantPn {
			t.Errorf("got %d want %d", gotPn, wantPn)
		}
	})
}

func TestPageSet(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		pager, err := New(true, "")
		if err != nil {
			t.Fatal(err)
		}
		p := pager.GetPage(1)

		p.SetValue([]byte{2}, []byte{'g', 'r', 'e', 'g'})
		p.SetValue([]byte{1}, []byte{'c', 'a', 'r', 'l'})
		p.SetValue([]byte{3}, []byte{'j', 'i', 'l', 'l', 'i', 'a', 'n'})

		ExpectUint16(t, p.content, 13, 3)
		ExpectUint16(t, p.content, 15, 4091)
		ExpectUint16(t, p.content, 17, 4092)
		ExpectUint16(t, p.content, 19, 4086)
		ExpectUint16(t, p.content, 21, 4087)
		ExpectUint16(t, p.content, 23, 4078)
		ExpectUint16(t, p.content, 25, 4079)

		ExpectByteArray(t, p.content, 4078, []byte{3})
		ExpectByteArray(t, p.content, 4079, []byte{'j', 'i', 'l', 'l', 'i', 'a', 'n'})
		ExpectByteArray(t, p.content, 4086, []byte{2})
		ExpectByteArray(t, p.content, 4087, []byte{'g', 'r', 'e', 'g'})
		ExpectByteArray(t, p.content, 4091, []byte{1})
		ExpectByteArray(t, p.content, 4092, []byte{'c', 'a', 'r', 'l'})
	})

	t.Run("set update", func(t *testing.T) {
		pager, err := New(true, "")
		if err != nil {
			t.Fatal(err)
		}
		p := pager.GetPage(1)

		p.SetValue([]byte{1}, []byte{'c', 'a', 'r', 'l'})
		p.SetValue([]byte{1}, []byte{'r', 'o', 'l', 'f'})

		ExpectUint16(t, p.content, 13, 1)
		ExpectUint16(t, p.content, 15, 4091)
		ExpectUint16(t, p.content, 17, 4092)

		ExpectByteArray(t, p.content, 4091, []byte{1})
		ExpectByteArray(t, p.content, 4092, []byte{'r', 'o', 'l', 'f'})
	})
}

func TestGet(t *testing.T) {

	t.Run("get", func(t *testing.T) {
		pager, err := New(true, "")
		if err != nil {
			t.Fatal(err)
		}
		p := pager.GetPage(1)
		n := []byte{'o', 'k', 'i', 'e'}
		p.SetValue([]byte{3}, []byte{'j', 'a', 'n', 'i', 'c', 'e'})
		p.SetValue([]byte{1}, n)
		p.SetValue([]byte{5}, []byte{'m', 'a', 't', 'i', 'l', 'd', 'a'})

		ret, found := p.GetValue([]byte{1})

		if !bytes.Equal(ret, n) {
			t.Errorf("expected %v got %v", n, ret)
		}
		if !found {
			t.Error("expected found")
		}
	})

	t.Run("get not found", func(t *testing.T) {
		pager, err := New(true, "")
		if err != nil {
			t.Fatal(err)
		}
		p := pager.GetPage(1)

		_, found := p.GetValue([]byte{1})

		if found {
			t.Error("expected not found")
		}
	})
}

func ExpectUint16(t *testing.T, content []byte, start int, expected uint16) {
	e := make([]byte, 2)
	binary.LittleEndian.PutUint16(e, expected)
	if !bytes.Equal(content[start:start+2], e) {
		t.Errorf("expected %v got %v at range start %d end %d", e, content[start:start+2], start, start+2)
	}
}

func ExpectByteArray(t *testing.T, content []byte, start int, expeted []byte) {
	end := start + len(expeted)
	if !bytes.Equal(content[start:end], expeted) {
		t.Errorf("expected %v got %v at range start %d end %d", expeted, content[start:end], start, end)
	}
}
