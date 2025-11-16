package cache

import "testing"

func TestCache(t *testing.T) {
	c := NewLRU(5, 0)
	c.Add(5, []byte{5})
	c.Add(8, []byte{8})
	c.Add(12, []byte{12})
	c.Add(21, []byte{21})
	c.Add(240, []byte{240})

	c.Get(5)
	c.Get(12)
	c.Get(8)
	c.Get(240)

	c.Add(241, []byte{241})

	if cl := len(c.cache); cl != 5 {
		t.Fatalf("expected cache size 5 got %d", cl)
	}
	if _, ok := c.cache[5]; !ok {
		t.Fatal("expected cache[5] to be ok")
	}
	if _, ok := c.cache[12]; !ok {
		t.Fatal("expected cache[12] to be ok")
	}
	if _, ok := c.cache[8]; !ok {
		t.Fatal("expected cache[8] to be ok")
	}
	if _, ok := c.cache[240]; !ok {
		t.Fatal("expected cache[240] to be ok")
	}
	if _, ok := c.cache[241]; !ok {
		t.Fatal("expected cache[241] to be ok")
	}
}

func TestVersion(t *testing.T) {
	v1 := 0
	v2 := 1
	c := NewLRU(5, v1)
	c.Add(1, []byte{1})
	_, hit := c.Get(1)
	if !hit {
		t.Fatal("expected hit to be true")
	}
	c.SetVersion(v2)
	c.Validate(v1)
	_, hit = c.Get(1)
	if hit {
		t.Fatal("expected hit to be false")
	}
}
