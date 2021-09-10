package atm

import (
	"container/heap"
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

	index      map[string]*CacheItem
	itemHeap   *CacheItemHeap
	writeFunc  func(filePath string, data []byte) error
	readerFunc func(filePath string) ([]byte, error)

	maxSizeInBytes int
	sizeInBytes    int

	mu sync.RWMutex
}

func NewCache(basePath string, maxSizeInBytes int) *Cache {
	c := &Cache{
		basePath:       basePath,
		maxSizeInBytes: maxSizeInBytes,
		writeFunc:      write,
		readerFunc:     read,
		itemHeap:       &CacheItemHeap{},
	}

	c.initialize()

	return c
}

func (c *Cache) initialize() {
	c.index = map[string]*CacheItem{}
	heap.Init(c.itemHeap)
	//todo: walk base folder and reload index
}

func (c *Cache) toFilePath(key string, t time.Time) string {
	return toFilePath(c.basePath, key, t)
}

func toFilePath(basePath, key string, t time.Time) string {
	name := fmt.Sprintf("%s-%s", key, t.Format(DateFormat))
	return path.Join(basePath, name)
}

func (c *Cache) Write(key string, t time.Time, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//todo: check if new data fix cache remaining space.
	//		if not purge until it fit.
	c.sizeInBytes += len(data)
	filePath := c.toFilePath(key, t)
	item := newCacheItem(key, filePath, t)
	err := c.writeFunc(filePath, data)
	if err != nil {
		return fmt.Errorf("writing file: %s: %w", filePath, err)
	}
	c.index[key] = item
	heap.Push(c.itemHeap, item)

	return nil
}

func (c *Cache) purgeWithLock(neededSpace int) error { //this func should always be call within a cache lock
	//todo: evict items until cache has enough free space
	return nil
}

func (c *Cache) Evict(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictWithLock(key)
}

func (c *Cache) evictWithLock(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//todo: remove from maps
	//todo: remove from sortedItems
	//todo: trigger file delete
	//todo: calculate new cache size in bytes
}

func (c *Cache) Read(key string) (data []byte, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return
}

func write(filePath string, data []byte) error {
	return nil
}

func read(filePath string) ([]byte, error) {
	return nil, nil
}

type CacheItem struct {
	key      string
	time     time.Time
	filePath string
}

func newCacheItem(key string, filePath string, time time.Time) *CacheItem {
	return &CacheItem{
		key:      key,
		filePath: filePath,
		time:     time,
	}
}

func cacheItemFromFileName(filePath string) (key string, item *CacheItem) {
	//path := f.Name()
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

	item = newCacheItem(key, filePath, t)

	return
}
