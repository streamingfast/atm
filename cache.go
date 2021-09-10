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

	index      map[string]*CacheItem
	itemHeap   *CacheItemHeap
	writeFunc  func(filePath string, data []byte) error
	readerFunc func(filePath string) ([]byte, error)

	maxSizeInBytes int
	sizeInBytes    int

	mu      sync.RWMutex
	cacheIO CacheIO
}

func NewCache(basePath string, maxSizeInBytes int, cacheIO CacheIO) *Cache {
	c := &Cache{
		basePath:       basePath,
		maxSizeInBytes: maxSizeInBytes,
		itemHeap:       &CacheItemHeap{},
		cacheIO:        cacheIO,
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

	c.purgeWithLock(len(data))

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

func (c *Cache) purgeWithLock(neededSpace int) { //this func should always be call within a cache lock
	freeSpace := c.maxSizeInBytes - c.sizeInBytes
	if freeSpace >= neededSpace {
		return
	}

	for freeSpace < neededSpace {
		c.evictWithLock()
		freeSpace = c.maxSizeInBytes - c.sizeInBytes
	}

	return
}

func (c *Cache) evictWithLock() {
	removed := heap.Pop(c.itemHeap)
	if removed == nil {
		return
	}

	removedItem := removed.(*CacheItem)
	c.sizeInBytes -= removedItem.size

	delete(c.index, removedItem.key)

	go func() {
		_ = c.cacheIO.Delete(removedItem.filePath)
		//todo: log err as warning here
	}()
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
	key      string
	size     int
	time     time.Time
	filePath string
}

func newCacheItem(key string, filePath string, size int, time time.Time) *CacheItem {
	return &CacheItem{
		key:      key,
		filePath: filePath,
		size:     size,
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
