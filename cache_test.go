package atm

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Write(t *testing.T) {
	aTime, err := time.Parse(DateFormat, DateFormat)
	require.NoError(t, err)

	cases := []struct {
		name                string
		items               map[string][]byte
		expectedSizeInBytes int
		expectedIndexSize   int
		expectedIndex       map[string]*CacheItem
		expectedHeap        *CacheItemHeap
	}{
		{
			name: "one",
			items: map[string][]byte{
				"key.1": {1, 2, 3},
			},
			expectedSizeInBytes: 3,
			expectedIndexSize:   1,
			expectedIndex: map[string]*CacheItem{
				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), aTime),
			},
			expectedHeap: &CacheItemHeap{
				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), aTime),
			},
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
				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), aTime),
				"key.2": newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime), aTime),
			},
			expectedHeap: &CacheItemHeap{
				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), aTime),
				newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime), aTime),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cache := NewCache("/tmp", 1000)
			cache.writeFunc = func(filePath string, data []byte) error { return nil }
			cache.readerFunc = func(filePath string) ([]byte, error) { return nil, nil }

			for k, data := range c.items {
				err = cache.Write(k, aTime, data)
				require.NoError(t, err)
			}

			assert.Equal(t, c.expectedSizeInBytes, cache.sizeInBytes)
			require.Equal(t, c.expectedIndexSize, len(cache.index))
			require.Equal(t, c.expectedIndexSize, cache.itemHeap.Len())

			require.Equal(t, c.expectedIndex, cache.index)
			require.Equal(t, c.expectedHeap, cache.itemHeap)
		})
	}
}

func TestCache_cacheItemFromFileName(t *testing.T) {
	filePath := fmt.Sprintf("/tmp/cache/key.1-%s", DateFormat)
	k, ci := cacheItemFromFileName(filePath)

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