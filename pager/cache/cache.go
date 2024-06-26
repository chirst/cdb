package cache

import "slices"

// lruPageCache implements pageCache
type lruPageCache struct {
	cache map[int][]byte
	// evictList maintains an ordered list of keys currently in the cache. The
	// list is ordered by the least recently used item at the 0th index of the
	// list.
	evictList []int
	maxSize   int
}

// NewLRU creates a LRU (least recently used) cache. This cache takes a maxSize
// which determines how many items can be cached. When the maximum size of the
// cache is exceeded, the least recently used item will be evicted.
func NewLRU(maxSize int) *lruPageCache {
	return &lruPageCache{
		cache:     map[int][]byte{},
		evictList: []int{},
		maxSize:   maxSize,
	}
}

// Get returns a bool indicating if the key was found and the value for the key.
func (c *lruPageCache) Get(key int) (value []byte, hit bool) {
	v, ok := c.cache[key]
	if !ok {
		return nil, false
	}
	c.prioritize(key)
	return v, true
}

// Add adds the key to the cache and prioritizes it. If a collision occurs, the
// key will be prioritized and the value will be updated.
func (c *lruPageCache) Add(key int, value []byte) {
	if _, ok := c.cache[key]; ok {
		c.prioritize(key)
		c.cache[key] = value
		return
	}
	if c.maxSize == len(c.cache) {
		c.evict()
	}
	c.cache[key] = value
	c.evictList = append(c.evictList, key)
}

// Remove removes the key from the cache. If the key is not found it will be
// ignored.
func (c *lruPageCache) Remove(key int) {
	if _, ok := c.cache[key]; ok {
		delete(c.cache, key)
		i := slices.Index(c.evictList, key)
		c.evictList = slices.Delete(c.evictList, i, i+1)
	}
}

func (c *lruPageCache) prioritize(key int) {
	i := slices.Index(c.evictList, key)
	c.evictList = append(slices.Delete(c.evictList, i, i+1), key)
}

func (c *lruPageCache) evict() {
	evictKey := c.evictList[0]
	c.evictList = c.evictList[1:]
	delete(c.cache, evictKey)
}
