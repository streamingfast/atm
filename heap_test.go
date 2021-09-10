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

}
