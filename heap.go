package atm

import "container/heap"

func ByAge(h []*CacheItem, i, j int) bool {
	return h[i].createdAt.Before(h[j].createdAt)
}

func ByInsertionTime(h []*CacheItem, i, j int) bool {
	return h[i].insertedAt.Before(h[j].insertedAt)
}

type Heap struct {
	heap []*CacheItem
	less func(h []*CacheItem, i, j int) bool
}

func NewHeap(less func(h []*CacheItem, i, j int) bool) *Heap {
	h := &Heap{
		heap: []*CacheItem{},
		less: less,
	}

	return h
}

func (h Heap) Len() int {
	return len(h.heap)
}

func (h Heap) Less(i, j int) bool {
	return h.less(h.heap, i, j)
}

func (h Heap) Swap(i, j int) {
	if len(h.heap) == 0 {
		return
	}

	h.heap[i], h.heap[j] = h.heap[j], h.heap[i]
}

func (h *Heap) Push(x interface{}) {
	h.heap = append(h.heap, x.(*CacheItem))
}

func (h *Heap) Pop() interface{} {
	old := h.heap
	if len(old) == 0 {
		return nil
	}

	n := len(old)
	x := old[n-1]
	h.heap = old[0 : n-1]
	return x
}

func (h *Heap) Get(i int) *CacheItem {
	x := h.heap[i]
	return x
}

func (h *Heap) Peek() *CacheItem {
	if len(h.heap) == 0 {
		return nil
	}
	return h.heap[len(h.heap)-1]
}

func (h *Heap) Remove(key string) *CacheItem {
	var i int
	var found bool
	for ix, item := range h.heap {
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
