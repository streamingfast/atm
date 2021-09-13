package atm

import "container/heap"

func ByAge(h []*CacheItem, i, j int) bool {
	return h[i].itemDate.Before(h[j].itemDate)
}

func ByInsertionTime(h []*CacheItem, i, j int) bool {
	return h[i].insertedAt.Before(h[j].insertedAt)
}

type Heap struct {
	items          []*CacheItem
	sizeInBytes    int
	maxSizeInBytes int
	less           func(h []*CacheItem, i, j int) bool
}

func NewHeap(less func(h []*CacheItem, i, j int) bool, maxSizeInByte int) *Heap {
	h := &Heap{
		items:          []*CacheItem{},
		less:           less,
		maxSizeInBytes: maxSizeInByte,
	}

	return h
}

func (h Heap) Len() int {
	return len(h.items)
}

func (h Heap) Less(i, j int) bool {
	return h.less(h.items, i, j)
}

func (h Heap) Swap(i, j int) {
	if len(h.items) == 0 {
		return
	}

	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *Heap) FreeSpace() int {
	return h.maxSizeInBytes - h.sizeInBytes
}

func (h *Heap) Push(x interface{}) {
	cacheItem := x.(*CacheItem)
	h.sizeInBytes += cacheItem.size
	h.items = append(h.items, cacheItem)
}

func (h *Heap) Pop() interface{} {
	old := h.items
	if len(old) == 0 {
		return nil
	}

	n := len(old)
	ci := old[n-1]
	h.items = old[0 : n-1]
	h.sizeInBytes -= ci.size
	return ci
}

func (h *Heap) Get(i int) *CacheItem {
	x := h.items[i]
	return x
}

func (h *Heap) Peek() *CacheItem {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[len(h.items)-1]
}

func (h *Heap) Remove(key string) *CacheItem {
	var foundAtIndex int
	var found bool
	for index, cacheIem := range h.items {
		if cacheIem.key == key {
			found = true
			foundAtIndex = index
		}
	}

	if !found {
		return nil
	}

	cacheItem := heap.Remove(h, foundAtIndex).(*CacheItem)
	h.sizeInBytes -= cacheItem.size
	return cacheItem
}
