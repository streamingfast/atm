package atm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCacheIO struct {
	writeFunc  func(path string, data []byte) error
	readFunc   func(path string) ([]byte, error)
	deleteFunc func(path string) error
}

func newTestCacheIO() *testCacheIO {
	return &testCacheIO{
		writeFunc: func(path string, data []byte) error {
			return nil
		},
		readFunc: func(path string) ([]byte, error) {
			return nil, nil
		},
		deleteFunc: func(path string) error {
			return nil
		},
	}
}
func (t *testCacheIO) Write(path string, data []byte) error {
	return t.writeFunc(path, data)
}

func (t testCacheIO) Read(path string) ([]byte, error) {
	return t.readFunc(path)
}

func (t testCacheIO) Delete(path string) error {
	return t.deleteFunc(path)
}

func TestCache_Write(t *testing.T) {
	aTime, err := time.Parse(DateFormat, DateFormat)
	require.NoError(t, err)

	cases := []struct {
		name                string
		items               map[string][]byte
		expectedSizeInBytes int
		expectedIndexSize   int
		expectedIndex       map[string]*CacheItem
		expectedHeap        *Heap
		expectedWriteCount  int
	}{
		{
			name: "one",
			items: map[string][]byte{
				"key.1": {1, 2, 3},
			},
			expectedSizeInBytes: 3,
			expectedIndexSize:   1,
			expectedIndex: map[string]*CacheItem{
				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
			},
			expectedHeap: &Heap{
				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
			},
			expectedWriteCount: 1,
		},
		{
			name: "two",
			items: map[string][]byte{
				"key.1": {1, 2, 3},
				"key.2": {4, 5, 6},
			},
			expectedSizeInBytes: 6,
			expectedIndexSize:   2,
			expectedIndex: map[string]*CacheItem{
				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
				"key.2": newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime), 3, aTime),
			},
			expectedHeap: &Heap{
				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
				newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime), 3, aTime),
			},
			expectedWriteCount: 2,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cacheIO := newTestCacheIO()
			writeCount := 0
			cacheIO.writeFunc = func(path string, data []byte) error {
				writeCount++
				return nil
			}
			cache := NewCache("/tmp", 1000, cacheIO)

			for k, data := range c.items {
				err = cache.Write(k, aTime, data)
				require.NoError(t, err)
			}

			require.Equal(t, c.expectedWriteCount, writeCount)

			assert.Equal(t, c.expectedSizeInBytes, cache.sizeInBytes)
			require.Equal(t, c.expectedIndexSize, len(cache.index))
			require.Equal(t, c.expectedIndexSize, cache.accessHeap.Len())

			require.Equal(t, c.expectedIndex, cache.index)
			require.Equal(t, c.expectedHeap, cache.accessHeap)
		})
	}
}
func TestCache_Purge(t *testing.T) {
	aTime, err := time.Parse(DateFormat, DateFormat)
	require.NoError(t, err)

	cases := []struct {
		name                string
		maxSize             int
		index               map[string]*CacheItem
		heap                *Heap
		sizeInBytes         int
		requestedSpace      int
		expectedSizeInBytes int
		expectedIndexSize   int
		expectedIndex       map[string]*CacheItem
		expectedHeap        *Heap
	}{
		{
			name: "sunny path",
			index: map[string]*CacheItem{
				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
			},
			heap: &Heap{
				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
			},
			maxSize:             3,
			sizeInBytes:         3,
			requestedSpace:      1,
			expectedSizeInBytes: 0,
			expectedIndexSize:   0,
			expectedIndex:       map[string]*CacheItem{},
			expectedHeap:        &Heap{},
		},
		{
			name: "keep 1 item",
			index: map[string]*CacheItem{
				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
				"key.2": newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
			},
			heap: &Heap{
				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
				newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
			},
			maxSize:             6,
			sizeInBytes:         6,
			requestedSpace:      1,
			expectedSizeInBytes: 3,
			expectedIndexSize:   1,
			expectedIndex: map[string]*CacheItem{
				"key.2": newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
			},
			expectedHeap: &Heap{
				newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := NewCache("/tmp", c.maxSize, newTestCacheIO())

			cache.index = c.index
			cache.accessHeap = c.heap
			cache.sizeInBytes = c.sizeInBytes

			cache.purgeWithLock(1)

			assert.Equal(t, c.expectedSizeInBytes, cache.sizeInBytes)
			require.Equal(t, c.expectedIndexSize, len(cache.index))
			require.Equal(t, c.expectedIndexSize, cache.accessHeap.Len())

			require.Equal(t, c.expectedIndex, cache.index)
			require.Equal(t, c.expectedHeap, cache.accessHeap)
		})
	}

}

func TestCache_cacheItemFromFileName(t *testing.T) {
	filePath := fmt.Sprintf("/tmp/cache/key.1-%s", DateFormat)
	size := 1
	k, ci := cacheItemFromFileName(filePath, size)

	expectedTime, err := time.Parse(DateFormat, DateFormat)
	require.NoError(t, err)

	require.Equal(t, "key.1", k)
	require.Equal(t, expectedTime, ci.time)
	require.Equal(t, filePath, ci.filePath)
}

func TestCache_toFilePath(t *testing.T) {
	cache := &Cache{basePath: "/tmp/cache"}

	itemTime, err := time.Parse(DateFormat, DateFormat)
	require.NoError(t, err)
	filePath := cache.toFilePath("key.1", itemTime)

	require.Equal(t, "/tmp/cache/key.1-20060102T1504059999", filePath)
}
