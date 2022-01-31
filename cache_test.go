package atm

import (
	"container/heap"
	"sort"
	"testing"
	"time"

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

var aTime time.Time

func init() {
	aTime, _ = time.Parse(DateFormat, DateFormat)
}

func ttime(addSecond int) time.Time {
	return aTime.Add(time.Duration(addSecond) * time.Second)
}

type testItem struct {
	key      string
	data     []byte
	itemDate time.Time
}

func newTestItem(key string, timeOffset int, dataLen int) *testItem {
	var data []byte
	for i := 0; i < dataLen; i++ {
		data = append(data, 0)
	}

	return &testItem{
		key:      key,
		data:     data,
		itemDate: ttime(timeOffset),
	}
}

func TestCache_Write(t *testing.T) {

	cases := []struct {
		name                    string
		items                   []*testItem
		maxRecentEntryBytes     int
		maxEntryByAgeBytes      int
		expectedIndex           []string
		expectedRecentEntryHeap []string
		expectedAgedRecentHeap  []string
		expectedWriteCount      int
		systemBlockSize         int
	}{
		{
			name:                "one",
			maxEntryByAgeBytes:  100,
			maxRecentEntryBytes: 3,
			items: []*testItem{
				newTestItem("key.0", 0, 3),
			},

			expectedIndex: []string{
				"key.0",
			},
			expectedRecentEntryHeap: []string{
				"key.0",
			},
			expectedAgedRecentHeap: nil,
			expectedWriteCount:     1,
			systemBlockSize:        0,
		},
		{
			name: "two",
			items: []*testItem{
				newTestItem("key.0", 1, 3),
				newTestItem("key.1", 0, 3),
			},
			maxEntryByAgeBytes:  100,
			maxRecentEntryBytes: 6,
			expectedIndex: []string{
				"key.0",
				"key.1",
			},
			expectedRecentEntryHeap: []string{
				"key.0",
				"key.1",
			},
			expectedAgedRecentHeap: nil,
			expectedWriteCount:     2,
			systemBlockSize:        0,
		},
		{
			name: "two one age",
			items: []*testItem{
				newTestItem("key.0", 2, 3),
				newTestItem("key.1", 1, 3),
				newTestItem("key.2", 0, 3),
			},
			maxRecentEntryBytes: 6,
			maxEntryByAgeBytes:  6,
			expectedIndex: []string{
				"key.0",
				"key.1",
				"key.2",
			},
			expectedRecentEntryHeap: []string{
				"key.1",
				"key.2",
			},
			expectedAgedRecentHeap: []string{
				"key.0",
			},
			expectedWriteCount: 3,
			systemBlockSize:    0,
		},
		{
			name: "2 recent, 2 age, 2 bye bye",
			items: []*testItem{
				newTestItem("key.0", 4, 3),
				newTestItem("key.1", 3, 3),
				newTestItem("key.2", 2, 3),
				newTestItem("key.3", 1, 3),
				newTestItem("key.4", 0, 3),
			},
			maxRecentEntryBytes: 6,
			maxEntryByAgeBytes:  6,
			expectedIndex: []string{
				"key.0",
				"key.1",
				"key.3",
				"key.4",
			},
			expectedRecentEntryHeap: []string{
				"key.3",
				"key.4",
			},
			expectedAgedRecentHeap: []string{
				"key.1",
				"key.0",
			},
			expectedWriteCount: 5,
			systemBlockSize:    0,
		},
		{
			name: "With 1 fat",
			items: []*testItem{
				newTestItem("key.0", 0, 3), // old block
				newTestItem("key.1", 1, 3),
				newTestItem("key.2", 2, 3),
				newTestItem("key.3", 3, 3),
				newTestItem("key.4", 4, 3),
				newTestItem("key.5", 5, 4), // recent block
			},
			maxRecentEntryBytes: 6,
			maxEntryByAgeBytes:  6,
			expectedIndex: []string{
				"key.3",
				"key.4",
				"key.5",
			},
			expectedRecentEntryHeap: []string{
				"key.5",
			},
			expectedAgedRecentHeap: []string{
				"key.3",
				"key.4",
			},
			expectedWriteCount: 6,
			systemBlockSize:    0,
		},
		{
			name: "testing system block size",
			items: []*testItem{
				newTestItem("key.0", 0, 1), // old block
				newTestItem("key.1", 1, 1),
				newTestItem("key.2", 2, 1),
				newTestItem("key.3", 3, 1),
				newTestItem("key.4", 4, 1),
				newTestItem("key.5", 5, 1), // recent block
			},
			maxRecentEntryBytes: 3 + (3 * 5), // 3 blocks with their extra file size padding
			maxEntryByAgeBytes:  2 + (2 * 5), // 2 blocks " " " " "
			expectedIndex: []string{
				//"key.0",
				"key.1",
				"key.2",
				"key.3",
				"key.4",
				"key.5",
			},
			expectedRecentEntryHeap: []string{
				"key.3",
				"key.4",
				"key.5",
			},
			expectedAgedRecentHeap: []string{
				//"key.0",
				"key.1",
				"key.2",
			},
			expectedWriteCount: 6,
			systemBlockSize:    5,
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
			cacheIO.deleteFunc = func(path string) error {
				return nil
			}

			SystemBlockSize = c.systemBlockSize
			cache := NewCache("/tmp", c.maxRecentEntryBytes, c.maxEntryByAgeBytes, cacheIO)

			var count = 0
			for _, testItem := range c.items {
				_, err := cache.Write(testItem.key, testItem.itemDate, ttime(count), testItem.data)
				require.NoError(t, err)
				count++
			}

			require.Equal(t, c.expectedWriteCount, writeCount)

			var indexItems []*CacheItem
			for _, item := range cache.index {
				indexItems = append(indexItems, item)
			}

			sort.Slice(indexItems, func(i, j int) bool {
				return ByInsertionTime(indexItems, i, j)
			})

			for i, key := range c.expectedIndex {
				require.Equal(t, key, indexItems[i].key)
			}

			if c.expectedRecentEntryHeap != nil {
				for _, key := range c.expectedRecentEntryHeap {
					popped := heap.Pop(cache.recentEntryHeap).(*CacheItem)
					require.Equal(t, key, popped.key)
				}
			} else {
				require.Equal(t, cache.recentEntryHeap.Len(), 0)
			}

			if c.expectedAgedRecentHeap != nil {
				for _, key := range c.expectedAgedRecentHeap {
					popped := heap.Pop(cache.ageHeap).(*CacheItem)
					require.Equal(t, key, popped.key)
				}
			} else {
				require.Equal(t, cache.ageHeap.Len(), 0)
			}
		})
	}
}

//func TestCache_Purge(t *testing.T) {
//	aTime, err := time.Parse(DateFormat, DateFormat)
//	require.NoError(t, err)
//
//	cases := []struct {
//		name                string
//		maxSize             int
//		index               map[string]*CacheItem
//		heap                *Heap
//		sizeInBytes         int
//		requestedSpace      int
//		expectedSizeInBytes int
//		expectedIndexSize   int
//		expectedIndex       map[string]*CacheItem
//		expectedHeap        *Heap
//	}{
//		{
//			name: "sunny path",
//			index: map[string]*CacheItem{
//				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
//			},
//			heap: &Heap{
//				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
//			},
//			maxSize:             3,
//			sizeInBytes:         3,
//			requestedSpace:      1,
//			expectedSizeInBytes: 0,
//			expectedIndexSize:   0,
//			expectedIndex:       map[string]*CacheItem{},
//			expectedHeap:        &Heap{},
//		},
//		{
//			name: "keep 1 item",
//			index: map[string]*CacheItem{
//				"key.1": newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
//				"key.2": newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
//			},
//			heap: &Heap{
//				newCacheItem("key.1", toFilePath("/tmp", "key.1", aTime), 3, aTime),
//				newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
//			},
//			maxSize:             6,
//			sizeInBytes:         6,
//			requestedSpace:      1,
//			expectedSizeInBytes: 3,
//			expectedIndexSize:   1,
//			expectedIndex: map[string]*CacheItem{
//				"key.2": newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
//			},
//			expectedHeap: &Heap{
//				newCacheItem("key.2", toFilePath("/tmp", "key.2", aTime.Add(time.Second)), 3, aTime.Add(time.Second)),
//			},
//		},
//	}
//
//	for _, c := range cases {
//		t.Run(c.name, func(t *testing.T) {
//			cache := NewCache("/tmp", c.maxSize, newTestCacheIO())
//
//			cache.index = c.index
//			cache.accessHeap = c.heap
//			cache.sizeInBytes = c.sizeInBytes
//
//			cache.purgeWithLock(1)
//
//			assert.Equal(t, c.expectedSizeInBytes, cache.sizeInBytes)
//			require.Equal(t, c.expectedIndexSize, len(cache.index))
//			require.Equal(t, c.expectedIndexSize, cache.accessHeap.Len())
//
//			require.Equal(t, c.expectedIndex, cache.index)
//			require.Equal(t, c.expectedHeap, cache.accessHeap)
//		})
//	}
//
//}
//
//func TestCache_cacheItemFromFileName(t *testing.T) {
//	filePath := fmt.Sprintf("/tmp/cache/key.1-%s", DateFormat)
//	size := 1
//	k, ci := cacheItemFromFile(filePath, size)
//
//	expectedTime, err := time.Parse(DateFormat, DateFormat)
//	require.NoError(t, err)
//
//	require.Equal(t, "key.1", k)
//	require.Equal(t, expectedTime, ci.time)
//	require.Equal(t, filePath, ci.filePath)
//}
//
//func TestCache_toFilePath(t *testing.T) {
//	cache := &Cache{basePath: "/tmp/cache"}
//
//	itemTime, err := time.Parse(DateFormat, DateFormat)
//	require.NoError(t, err)
//	filePath := cache.toFilePath("key.1", itemTime)
//
//	require.Equal(t, "/tmp/cache/key.1-20060102T1504059999", filePath)
//}
