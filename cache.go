package atm

import (
	"container/heap"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const DateFormat = "20060102T1504059999"

type Cache struct {
	basePath string

	index           map[string]*CacheItem
	recentEntryHeap *Heap
	ageHeap         *Heap

	mu      sync.RWMutex
	cacheIO CacheIO
}

func NewCache(basePath string, maxRecentEntryBytes, maxEntryByAgeBytes int, cacheIO CacheIO) *Cache {
	c := &Cache{
		basePath: basePath,

		recentEntryHeap: NewHeap(ByInsertionTime, maxRecentEntryBytes),
		ageHeap:         NewHeap(ByAge, maxEntryByAgeBytes),
		cacheIO:         cacheIO,
	}

	heap.Init(c.ageHeap)
	heap.Init(c.recentEntryHeap)

	c.initialize()

	return c
}

func (c *Cache) initialize() {
	c.index = map[string]*CacheItem{}

	//todo: walk base folder and reload index
}

func (c *Cache) toFilePath(key string, t time.Time) string {
	return toFilePath(c.basePath, key, t)
}

func toFilePath(basePath, key string, t time.Time) string {
	name := fmt.Sprintf("%s-%s", key, t.Format(DateFormat))
	return path.Join(basePath, name)
}

func (c *Cache) Write(key string, itemDate time.Time, data []byte) (*CacheItem, error) {
	return c.write(key, itemDate, time.Now(), data)
}

func (c *Cache) write(key string, itemDate, insertionTime time.Time, data []byte) (*CacheItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	evictedCacheItems := c.purgeWithLock(c.recentEntryHeap, len(data))

	for _, evicted := range evictedCacheItems {
		if c.ageHeap.FreeSpace() >= evicted.size { //we need space
			c.ageHeap.Push(evicted)
			continue
		}

		peek := c.ageHeap.Peek()
		if peek.itemDate.Before(evicted.itemDate) { //evicted item is older then last age item so we remove it
			ageEvictedItems := c.purgeWithLock(c.ageHeap, len(data))
			for _, ageEvicted := range ageEvictedItems {
				delete(c.index, ageEvicted.key)
				go func() {
					_ = c.cacheIO.Delete(evicted.filePath)
					//todo: log err as warning here
				}()
			}
			c.ageHeap.Push(evicted)
		} else {
			delete(c.index, evicted.key)
		}
	}

	filePath := c.toFilePath(key, itemDate)
	item := newCacheItem(key, filePath, len(data), itemDate, insertionTime)

	err := c.cacheIO.Write(filePath, data)
	if err != nil {
		return nil, fmt.Errorf("writing file: %s: %w", filePath, err)
	}

	c.index[key] = item
	heap.Push(c.recentEntryHeap, item)

	return item, err
}

func (c *Cache) purgeWithLock(h *Heap, neededSpace int) (evictedCacheItems []*CacheItem) { //this func should always be call within a cache lock
	freeSpace := h.FreeSpace()
	if h.FreeSpace() >= neededSpace {
		return
	}

	for freeSpace < neededSpace {
		evicted := c.evictWithLock(h)
		evictedCacheItems = append(evictedCacheItems, evicted)
		freeSpace = h.FreeSpace()
	}

	return
}

func (c *Cache) evictWithLock(h *Heap) *CacheItem {
	removed := heap.Pop(h)
	if removed == nil {
		return nil
	}

	return removed.(*CacheItem)

}

var NotFoundError = errors.New("not found")

func (c *Cache) Read(key string) (data []byte, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if ci, found := c.index[key]; found {
		return c.cacheIO.Read(ci.filePath)
	}

	return nil, NotFoundError
}

type CacheItem struct {
	key        string
	size       int
	itemDate   time.Time
	insertedAt time.Time
	filePath   string
}

func newCacheItem(key string, filePath string, size int, itemDate, insertedAt time.Time) *CacheItem {
	return &CacheItem{
		key:        key,
		filePath:   filePath,
		size:       size,
		itemDate:   itemDate,
		insertedAt: insertedAt,
	}
}

func cacheItemFromFileName(filePath string, size int) (key string, item *CacheItem) {
	name := filepath.Base(filePath)

	parts := strings.Split(name, "-")
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid file name, expected 3 parts got %d", len(parts)))
	}
	key = parts[0]
	t, err := time.Parse(DateFormat, parts[1])
	if err != nil {
		panic(err)
	}

	//todo: check file time
	item = newCacheItem(key, filePath, size, t, time.Now())

	return
}
