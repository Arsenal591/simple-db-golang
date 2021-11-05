package disk

import (
	"container/list"
	"sync"
)

type LRUReplacer struct {
	dataList list.List
	index    map[int]*list.Element
	mu       sync.Mutex
}

func NewLRUReplacer() *LRUReplacer {
	return &LRUReplacer{
		index: make(map[int]*list.Element),
	}
}

func (lru *LRUReplacer) Victim() (int, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if len(lru.index) == 0 {
		return 0, false
	}
	elem := lru.dataList.Back()
	frameId := elem.Value.(int)
	lru.dataList.Remove(elem)
	delete(lru.index, frameId)
	return frameId, true
}

func (lru *LRUReplacer) Add(frameId int) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if _, ok := lru.index[frameId]; ok {
		return
	}
	lru.dataList.PushFront(frameId)
	lru.index[frameId] = lru.dataList.Front()
}

func (lru *LRUReplacer) Remove(frameId int) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, ok := lru.index[frameId]; !ok {
		return
	} else {
		lru.dataList.Remove(elem)
		delete(lru.index, frameId)
	}
}

func (lru *LRUReplacer) Size() int {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return len(lru.index)
}
