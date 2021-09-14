package atm

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeapPopOrder(t *testing.T) {
	h := &Heap{
		less: ByAge,
	}
	heap.Init(h)

	oldest := &CacheItem{
		key:      "oldest",
		itemDate: time.Now().Add(-1 * 60 * time.Second),
		filePath: "/foo/1",
	}
	newest := &CacheItem{
		key:      "newest",
		itemDate: time.Now().Add(60 * time.Second),
		filePath: "/foo/3",
	}
	middle := &CacheItem{
		key:      "middle",
		itemDate: time.Now(),
		filePath: "/foo/2",
	}

	heap.Push(h, oldest)
	heap.Push(h, newest)
	heap.Push(h, middle)

	peeked1 := h.Peek()
	res1 := heap.Pop(h)
	peeked2 := h.Peek()
	res2 := heap.Pop(h)
	peeked3 := h.Peek()
	res3 := heap.Pop(h)
	peeked4 := h.Peek()
	res4 := heap.Pop(h)

	assert.Equal(t, peeked1, res1)
	assert.Equal(t, peeked2, res2)
	assert.Equal(t, peeked3, res3)
	assert.Nil(t, peeked4)
	assert.Equal(t, res1.(*CacheItem).key, "oldest")
	assert.Equal(t, res2.(*CacheItem).key, "middle")
	assert.Equal(t, res3.(*CacheItem).key, "newest")
	assert.Nil(t, res4)
}
