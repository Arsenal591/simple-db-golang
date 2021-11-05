package disk

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/ncw/directio"
	log "github.com/sirupsen/logrus"

	"simple-db-golang/src/common"
)

type BufferPoolManager struct {
	size        int
	pages       []Page
	replacer    Replacer
	freeList    list.List
	pageTable   map[common.PageId]int
	diskManager *DiskManager
	mu          sync.Mutex
}

func NewBufferPoolManager(size int, diskManager *DiskManager, replacer Replacer) *BufferPoolManager {
	bpm := &BufferPoolManager{
		size:        size,
		pages:       make([]Page, size),
		replacer:    replacer,
		pageTable:   make(map[common.PageId]int),
		diskManager: diskManager,
	}
	for i := 0; i < size; i++ {
		bpm.pages[i] = Page{
			data:     directio.AlignedBlock(pageSize),
			pageId:   common.InvalidPageId,
			pinCount: 0,
			isDirty:  false,
		}
		bpm.freeList.PushBack(i)
	}
	return bpm
}

func (bpm *BufferPoolManager) FetchPage(pageId common.PageId) (*Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameId, ok := bpm.pageTable[pageId]; ok {
		bpm.replacer.Remove(frameId)
		page := &bpm.pages[frameId]
		page.pinCount += 1
		return page, nil
	}
	frameId, found := bpm.findAvailablePage()
	if !found {
		log.Warnf("Buffer pool is full.")
		return nil, fmt.Errorf("Buffer pool is full.")
	}
	page := &bpm.pages[frameId]
	oldPageId := page.PageId()
	if page.IsDirty() {
		if err := bpm.diskManager.WritePage(oldPageId, page.Data()); err != nil {
			log.WithError(err).Fatalf("Cannot write page %d back.", oldPageId)
		}
		page.isDirty = false
	}
	if err := bpm.diskManager.ReadPage(pageId, page.Data()); err != nil {
		log.WithError(err).Warnf("Cannot read page %d from disk.", pageId)
		return nil, err
	}

	page.pageId = pageId
	page.pinCount = 1
	delete(bpm.pageTable, oldPageId)
	bpm.pageTable[pageId] = frameId
	return page, nil
}

func (bpm *BufferPoolManager) UnpinPage(pageId common.PageId, isDirty bool) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameId, ok := bpm.pageTable[pageId]; !ok {
		log.Warnf("Trying to unpin page %d, but the page is not in the buffer.", pageId)
	} else {
		page := &bpm.pages[frameId]
		if page.PinCount() > 0 {
			page.pinCount--
			page.isDirty = page.isDirty || isDirty
			if page.pinCount == 0 {
				bpm.replacer.Add(frameId)
			}
		} else {
			log.Warnf("Trying to unpin a page %d, but page's pin count is zero. ", pageId)
		}
	}
}

func (bpm *BufferPoolManager) FlushPage(pageId common.PageId) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameId, ok := bpm.pageTable[pageId]; !ok {
		log.Warnf("Page %d is not in buffer. Cannot flush page.", pageId)
		return nil
	} else {
		page := &bpm.pages[frameId]
		if page.isDirty {
			if err := bpm.diskManager.WritePage(page.PageId(), page.Data()); err != nil {
				log.WithError(err).Errorf("Cannot flush page %d.", page.PageId())
				return err
			}
			page.isDirty = false
		}
		return nil
	}
}

func (bpm *BufferPoolManager) NewPage() (*Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameId, found := bpm.findAvailablePage()
	if !found {
		log.Warnf("Buffer pool is full.")
		return nil, fmt.Errorf("Buffer pool is full.")
	}
	page := &bpm.pages[frameId]
	oldPageId := page.PageId()
	if page.IsDirty() {
		if err := bpm.diskManager.WritePage(oldPageId, page.Data()); err != nil {
			log.WithError(err).Fatalf("Cannot write page %d back.", oldPageId)
		}
		page.isDirty = false
	}
	newPageId, err := bpm.diskManager.AllocatePage()
	if err != nil {
		log.WithError(err).Errorf("Allocate page failed.")
		return nil, err
	}
	if err := bpm.diskManager.ReadPage(newPageId, page.Data()); err != nil {
		log.WithError(err).Errorf("Cannot read page %d from disk.", newPageId)
		return nil, err
	}
	page.pinCount = 1
	page.pageId = newPageId
	delete(bpm.pageTable, oldPageId)
	bpm.pageTable[newPageId] = frameId
	return page, nil
}

func (bpm *BufferPoolManager) DeletePage(pageId common.PageId) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameId, ok := bpm.pageTable[pageId]; !ok {
		return bpm.diskManager.DeallocatePage(pageId)
	} else {
		page := &bpm.pages[frameId]
		if page.PinCount() > 0 {
			return fmt.Errorf("Page %d is still pinned.", pageId)
		}
		if err := bpm.diskManager.DeallocatePage(pageId); err != nil {
			return err
		}
		page.pageId = common.InvalidPageId
		page.isDirty = false
		page.pinCount = 0
		delete(bpm.pageTable, pageId)
		bpm.replacer.Remove(frameId)
		bpm.freeList.PushBack(frameId)
		return nil
	}
}

func (bpm *BufferPoolManager) FlushAllPages() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	for _, frameId := range bpm.pageTable {
		page := &bpm.pages[frameId]
		if page.isDirty {
			if err := bpm.diskManager.WritePage(page.PageId(), page.Data()); err != nil {
				log.WithError(err).Errorf("Cannot flush page %d.", page.PageId())
				return err
			}
			page.isDirty = false
		}
	}
	return nil
}

func (bpm *BufferPoolManager) findAvailablePage() (int, bool) {
	if bpm.freeList.Len() == 0 {
		return bpm.replacer.Victim()
	}
	elem := bpm.freeList.Front()
	frameId := elem.Value.(int)
	bpm.freeList.Remove(elem)
	return frameId, true
}
