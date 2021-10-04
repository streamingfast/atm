package atm

import (
	"container/heap"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
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
		basePath:        basePath,
		index:           map[string]*CacheItem{},
		recentEntryHeap: NewHeap(ByInsertionTime, maxRecentEntryBytes),
		ageHeap:         NewHeap(ByAge, maxEntryByAgeBytes),
		cacheIO:         cacheIO,
	}

	heap.Init(c.ageHeap)
	heap.Init(c.recentEntryHeap)

	return c
}

func NewInitializedCache(basePath string, maxRecentEntryBytes, maxEntryByAgeBytes int, cacheIO CacheIO) (*Cache, error) {
	c := NewCache(basePath, maxEntryByAgeBytes, maxEntryByAgeBytes, cacheIO)

	return c.initialize()
}

func (c *Cache) initialize() (*Cache, error) {
	zlog.Info("initializing cache", zap.String("base_cache_path", c.basePath))
	c.index = map[string]*CacheItem{}

	files, err := ioutil.ReadDir(c.basePath)
	if err != nil {
		return c, fmt.Errorf("listing file of folder: %s : %w", c.basePath, err)
	}

	zlog.Info("load files to caches", zap.Int("file_count", len(files)))
	for _, f := range files {
		fmt.Println(f.Name())
		_, cacheItem := cacheItemFromFile(path.Join(c.basePath, f.Name()), f)
		_, err := c.write(cacheItem, []byte{}, true)
		if err != nil {
			return c, fmt.Errorf("writing cache item: %w", err)
		}
		zlog.Debug("file loaded to cache", zap.Stringer("cache_item", cacheItem))
	}
	return c, nil
}

func (c *Cache) toFilePath(key string, t time.Time) string {
	return toFilePath(c.basePath, key, t)
}

func toFilePath(basePath, key string, t time.Time) string {
	name := fmt.Sprintf("%s-%s", key, t.Format(DateFormat))
	return path.Join(basePath, name)
}

func (c *Cache) Write(key string, itemDate time.Time, insertionDate time.Time, data []byte) (*CacheItem, error) {
	filePath := c.toFilePath(key, itemDate)
	item := newCacheItem(key, filePath, len(data), itemDate, insertionDate)

	return c.write(item, data, false)
}

func (c *Cache) write(cacheItem *CacheItem, data []byte, skipWriteToFile bool) (*CacheItem, error) {
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

	if !skipWriteToFile {
		err := c.cacheIO.Write(cacheItem.filePath, data)
		if err != nil {
			return nil, fmt.Errorf("writing file: %s: %w", cacheItem.filePath, err)
		}
	}
	c.index[cacheItem.key] = cacheItem
	heap.Push(c.recentEntryHeap, cacheItem)

	return cacheItem, nil
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

func (i *CacheItem) String() string {
	return fmt.Sprintf("key: %s, size: %d: item date: %s, inserted at: %s, path: %s", i.key, i.size, i.itemDate, i.insertedAt, i.filePath)
}

func cacheItemFromFile(filePath string, fileInfo os.FileInfo) (key string, item *CacheItem) {

	parts := strings.Split(fileInfo.Name(), "-")
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid file name, expected 3 parts got %d", len(parts)))
	}
	key = parts[0]
	t, err := time.Parse(DateFormat, parts[1])
	if err != nil {
		panic(err)
	}

	item = newCacheItem(key, filePath, int(fileInfo.Size()), t, fileInfo.ModTime())

	return
}
