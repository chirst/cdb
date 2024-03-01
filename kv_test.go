package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPageHelpers(t *testing.T) {
	c := make([]byte, PAGE_SIZE)
	p := allocatePage(1, c)

	t.Run("get set internal", func(t *testing.T) {
		p.setType(PAGE_TYPE_INTERNAL)
		if res := p.getType(); res != PAGE_TYPE_INTERNAL {
			t.Errorf("want %d got %d", PAGE_TYPE_INTERNAL, res)
		}
	})

	t.Run("get set leaf", func(t *testing.T) {
		p.setType(PAGE_TYPE_LEAF)
		if res := p.getType(); res != PAGE_TYPE_LEAF {
			t.Errorf("want %d got %d", PAGE_TYPE_LEAF, res)
		}
	})

	t.Run("get set record count", func(t *testing.T) {
		want := uint16(2)
		p.setRecordCount(want)
		if res := p.getRecordCount(); res != want {
			t.Errorf("want %d got %d", want, res)
		}
	})
}

func TestPageSet(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		c := make([]byte, PAGE_SIZE)
		p := allocatePage(1, c)

		p.setValue([]byte{2}, []byte{'g', 'r', 'e', 'g'})
		p.setValue([]byte{1}, []byte{'c', 'a', 'r', 'l'})
		p.setValue([]byte{3}, []byte{'j', 'i', 'l', 'l', 'i', 'a', 'n'})

		ExpectUint16(t, p.content, 14, 3)
		ExpectUint16(t, p.content, 16, 4091)
		ExpectUint16(t, p.content, 18, 4092)
		ExpectUint16(t, p.content, 20, 4086)
		ExpectUint16(t, p.content, 22, 4087)
		ExpectUint16(t, p.content, 24, 4078)
		ExpectUint16(t, p.content, 26, 4079)

		ExpectByteArray(t, p.content, 4078, []byte{3})
		ExpectByteArray(t, p.content, 4079, []byte{'j', 'i', 'l', 'l', 'i', 'a', 'n'})
		ExpectByteArray(t, p.content, 4086, []byte{2})
		ExpectByteArray(t, p.content, 4087, []byte{'g', 'r', 'e', 'g'})
		ExpectByteArray(t, p.content, 4091, []byte{1})
		ExpectByteArray(t, p.content, 4092, []byte{'c', 'a', 'r', 'l'})
	})

	t.Run("set update", func(t *testing.T) {
		c := make([]byte, PAGE_SIZE)
		p := allocatePage(1, c)

		p.setValue([]byte{1}, []byte{'c', 'a', 'r', 'l'})
		p.setValue([]byte{1}, []byte{'r', 'o', 'l', 'f'})

		ExpectUint16(t, p.content, 14, 1)
		ExpectUint16(t, p.content, 16, 4091)
		ExpectUint16(t, p.content, 18, 4092)

		ExpectByteArray(t, p.content, 4091, []byte{1})
		ExpectByteArray(t, p.content, 4092, []byte{'r', 'o', 'l', 'f'})
	})
}

func TestGet(t *testing.T) {
	t.Run("get", func(t *testing.T) {
		c := make([]byte, PAGE_SIZE)
		p := allocatePage(1, c)
		n := []byte{'o', 'k', 'i', 'e'}
		p.setValue([]byte{3}, []byte{'j', 'a', 'n', 'i', 'c', 'e'})
		p.setValue([]byte{1}, n)
		p.setValue([]byte{5}, []byte{'m', 'a', 't', 'i', 'l', 'd', 'a'})

		ret, found := p.getValue([]byte{1})

		if !bytes.Equal(ret, n) {
			t.Errorf("expected %v got %v", n, ret)
		}
		if !found {
			t.Error("expected found")
		}
	})

	t.Run("get not found", func(t *testing.T) {
		c := make([]byte, PAGE_SIZE)
		p := allocatePage(1, c)

		_, found := p.getValue([]byte{1})

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
