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

	maxSizeInBytes int
	sizeInBytes    int

	mu      sync.RWMutex
	cacheIO CacheIO
}

func NewCache(basePath string, maxSizeInBytes int, cacheIO CacheIO) *Cache {
	c := &Cache{
		basePath:        basePath,
		maxSizeInBytes:  maxSizeInBytes,
		recentEntryHeap: NewHeap(ByInsertionTime),
		ageHeap:         NewHeap(ByAge),
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

func (c *Cache) Write(key string, t time.Time, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeWithLock(len(data))

	c.sizeInBytes += len(data)
	filePath := c.toFilePath(key, t)
	item := newCacheItem(key, filePath, len(data), t)

	err := c.cacheIO.Write(filePath, data)
	if err != nil {
		return fmt.Errorf("writing file: %s: %w", filePath, err)
	}

	c.index[key] = item
	heap.Push(c.recentEntryHeap, item)

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
	//todo: determine whether to add this removed item into the byAge heap
	removed := heap.Pop(c.recentEntryHeap)
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
	key        string
	size       int
	createdAt  time.Time
	insertedAt time.Time
	filePath   string
}

func newCacheItem(key string, filePath string, size int, createdAt time.Time) *CacheItem {
	return &CacheItem{
		key:        key,
		filePath:   filePath,
		size:       size,
		createdAt:  createdAt,
		insertedAt: time.Now(),
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

	item = newCacheItem(key, filePath, size, t)

	return
}
