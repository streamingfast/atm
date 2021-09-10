package atm

import "container/heap"

type CacheItemHeap []*CacheItem

func (h CacheItemHeap) Len() int {
	return len(h)
}

func (h CacheItemHeap) Less(i, j int) bool {
	return h[i].time.Before(h[j].time)
}

func (h CacheItemHeap) Swap(i, j int) {
	if len(h) == 0 {
		return
	}

	h[i], h[j] = h[j], h[i]
}

func (h *CacheItemHeap) Push(x interface{}) {
	*h = append(*h, x.(*CacheItem))
}

func (h *CacheItemHeap) Pop() interface{} {
	old := *h
	if len(old) == 0 {
		return nil
	}

	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *CacheItemHeap) Get(i int) *CacheItem {
	current := *h
	x := current[i]
	return x
}

func (h *CacheItemHeap) Remove(key string) *CacheItem {
	var i int
	var found bool
	for ix, item := range *h {
		if item.key == key {
			found = true
			i = ix
		}
	}

	if !found {
		return nil
	}

	return heap.Remove(h, i).(*CacheItem)
}
