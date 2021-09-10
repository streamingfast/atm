package atm

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeapPopOrder(t *testing.T) {
	h := &CacheItemHeap{}
	heap.Init(h)

	oldest := &CacheItem{
		key:      "oldest",
		time:     time.Now().Add(-1 * 60 * time.Second),
		filePath: "/foo/1",
	}
	newest := &CacheItem{
		key:      "newest",
		time:     time.Now().Add(60 * time.Second),
		filePath: "/foo/3",
	}
	middle := &CacheItem{
		key:      "middle",
		time:     time.Now(),
		filePath: "/foo/2",
	}

	heap.Push(h, oldest)
	heap.Push(h, newest)
	heap.Push(h, middle)

	res1 := heap.Pop(h)
	res2 := heap.Pop(h)
	res3 := heap.Pop(h)
	res4 := heap.Pop(h)

	assert.Equal(t, res1.(*CacheItem).key, "oldest")
	assert.Equal(t, res2.(*CacheItem).key, "middle")
	assert.Equal(t, res3.(*CacheItem).key, "newest")
	assert.Nil(t, res4)
}
