package atm

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
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

	go func() {
		for {
			select {
			case <-time.After(10 * time.Second):
				zlog.Info("cache stats",
					zap.Int("count_indexes", len(c.index)),
					zap.Int("count_recent entries", c.recentEntryHeap.Len()),
					zap.Int("count_age entries", c.ageHeap.Len()),
					zap.String("size_recent_heap", humanize.Bytes(uint64(c.recentEntryHeap.sizeInBytes))),
					zap.String("size_age_heap", humanize.Bytes(uint64(c.ageHeap.sizeInBytes))),
				)
			}
		}
	}()

	return c
}

func NewInitializedCache(basePath string, maxRecentEntryBytes, maxEntryByAgeBytes int, cacheIO CacheIO) (*Cache, error) {
	c := NewCache(basePath, maxRecentEntryBytes, maxEntryByAgeBytes, cacheIO)

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

	zlog.Debug("writing cache item", zap.Stringer("item", cacheItem))

	if item, ok := c.index[cacheItem.key]; ok {
		item.insertedAt = cacheItem.insertedAt
		return item, nil
	}

	evictedCacheItems := c.purgeWithLock(c.recentEntryHeap, len(data))
	if len(evictedCacheItems) > 0 {
		zlog.Debug("evicted from recent entry heap", zap.Reflect("items", evictedCacheItems))
	}

	for _, evicted := range evictedCacheItems {
		if c.ageHeap.FreeSpace() >= evicted.size { //we need space
			heap.Push(c.ageHeap, evicted)
			continue
		}

		peek := c.ageHeap.Peek()
		if peek.itemDate.Before(evicted.itemDate) { //evicted item is older then last age item so we remove it
			evictedAgeItems := c.purgeWithLock(c.ageHeap, len(data))
			for _, ageEvicted := range evictedAgeItems {
				delete(c.index, ageEvicted.key)
				go func(toDelete *CacheItem) {
					err := c.cacheIO.Delete(toDelete.filePath)
					if err != nil {
						zlog.Warn("failed to delete file", zap.String("file", toDelete.filePath), zap.Error(err))
					}
				}(ageEvicted)
			}
			heap.Push(c.ageHeap, evicted)
		} else {
			delete(c.index, evicted.key)
			go func(toDelete *CacheItem) {
				err := c.cacheIO.Delete(toDelete.filePath)
				if err != nil {
					zlog.Warn("too old to age heap : failed to delete file", zap.String("file", toDelete.filePath), zap.Error(err))
				}
			}(evicted)
		}
	}

	if !skipWriteToFile {
		err := c.cacheIO.Write(cacheItem.filePath, data)
		if err != nil {
			return nil, fmt.Errorf("writing file: %w", err)
		}
		zlog.Debug("wrote file", zap.String("path", cacheItem.filePath))
	}

	c.index[cacheItem.key] = cacheItem
	heap.Push(c.recentEntryHeap, cacheItem)

	return cacheItem, nil
}

func (c *Cache) purgeWithLock(h *Heap, neededSpace int) (evictedCacheItems []*CacheItem) { //this func should always be call within a cache lock
	freeSpace := h.FreeSpace()
	if freeSpace >= neededSpace {
		return
	}

	for freeSpace < neededSpace {
		evicted := c.evictWithLock(h)
		if evicted == nil {
			return
		}

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

func (c *Cache) Read(key string) (data []byte, found bool, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var cacheItem *CacheItem
	if cacheItem, found = c.index[key]; !found {
		return
	}

	zlog.Debug("reading cache item", zap.Stringer("item", cacheItem))

	data, err = c.cacheIO.Read(cacheItem.filePath)
	if err != nil {
		return
	}

	return
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
