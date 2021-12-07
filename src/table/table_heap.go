package table

import (
	"simple-db-golang/src/common"
	"simple-db-golang/src/disk"

	log "github.com/sirupsen/logrus"
)

const (
	heapFileHeaderPageId = common.PageId(1) // Simply assume the header page is always page ID 1.
)

type TableHeap struct {
	bufferPoolManager *disk.BufferPoolManager
}

func NewTableHeap(bufferPoolManager *disk.BufferPoolManager, isNew bool) *TableHeap {
	th := &TableHeap{
		bufferPoolManager: bufferPoolManager,
	}
	if isNew {
		if page, err := bufferPoolManager.NewPage(); err != nil {
			log.WithError(err).Fatalf("Cannot create table heap header page.")
		} else {
			if page.PageId() != heapFileHeaderPageId {
				log.Fatalf("Unexpected: header page id is not 1.")
			}
			header := createHeapFileHeader(page.Data())
			header.init()
			th.bufferPoolManager.UnpinPage(page.PageId(), true)
		}
	}
	return th
}

func (th *TableHeap) getHeaderPage(exclusive bool) *disk.Page {
	page, err := th.bufferPoolManager.FetchPage(heapFileHeaderPageId)
	if err != nil {
		log.WithError(err).Fatalf("Cannot fetch heap header page.")
	}
	if exclusive {
		page.Lock()
	} else {
		page.RLock()
	}
	return page
}

func (th *TableHeap) releaseHeaderPage(page *disk.Page, exclusive bool) {
	if exclusive {
		page.Unlock()
	} else {
		page.RUnlock()
	}
	th.bufferPoolManager.UnpinPage(heapFileHeaderPageId, exclusive)
}

func (th *TableHeap) Insert(record []byte) common.RID {
	internalLoop := func() (common.RID, bool) {
		headerPage := th.getHeaderPage(false)
		header := createHeapFileHeader(headerPage.Data())
		pageInfoList := header.getPageInfoList()

		for _, info := range pageInfoList {
			if int(info.leftSpace) >= len(record) {
				th.releaseHeaderPage(headerPage, false)
				rid, ok := th.insertIntoPage(record, info.pageId)
				if !ok {
					log.Warnf("Insert a record of length %d into page %d failed.", len(record), info.pageId)
					return common.RID{}, false
				} else {
					return rid, ok
				}
			}
		}
		th.releaseHeaderPage(headerPage, false)
		// insert into new page
		newPage, err := th.bufferPoolManager.NewPage()
		if err != nil {
			log.WithError(err).Fatalf("Cannot allocate new page.")
		}
		newPage.Lock()

		newTablePage := createTablePage(newPage.Data())
		newTablePage.init(newPage.PageId(), int32(len(newPage.Data())))
		rid, _ := newTablePage.Insert(record) // must be successful

		headerPage = th.getHeaderPage(true)
		header = createHeapFileHeader(headerPage.Data())
		header.pushPageInfo(pageInfo{
			pageId:    newPage.PageId(),
			leftSpace: newTablePage.getFreeSpaceForInsert(),
		})
		th.releaseHeaderPage(headerPage, true)

		newPage.Unlock()
		th.bufferPoolManager.UnpinPage(newPage.PageId(), true)
		return rid, true
	}
	for {
		rid, ok := internalLoop()
		if ok {
			return rid
		}
	}
}

func (th *TableHeap) insertIntoPage(record []byte, pageId common.PageId) (common.RID, bool) {
	page, err := th.bufferPoolManager.FetchPage(pageId)
	if err != nil {
		log.WithError(err).Fatalf("Cannot fetch page %d.", pageId)
	}
	page.Lock()
	tablePage := createTablePage(page.Data())
	rid, ok := tablePage.Insert(record)
	if !ok {
		page.Unlock()
		th.bufferPoolManager.UnpinPage(pageId, false)
		return common.RID{}, false
	}

	headerPage := th.getHeaderPage(true)
	header := createHeapFileHeader(headerPage.Data())
	header.setPageInfo(pageId, pageInfo{
		pageId:    pageId,
		leftSpace: tablePage.getFreeSpaceForInsert(),
	})
	th.releaseHeaderPage(headerPage, true)

	page.Unlock()
	th.bufferPoolManager.UnpinPage(pageId, true)
	return rid, true
}

func (th *TableHeap) Delete(rid common.RID) bool {
	headerPage := th.getHeaderPage(false)
	header := createHeapFileHeader(headerPage.Data())
	_, ok := header.getPageInfo(rid.PageId)
	th.releaseHeaderPage(headerPage, false)
	if !ok {

		return false
	}

	page, err := th.bufferPoolManager.FetchPage(rid.PageId)
	if err != nil {
		log.WithError(err).Fatalf("Unexpected page not found.")
	}
	page.Lock()

	tablePage := createTablePage(page.Data())
	deleted := tablePage.Delete(rid)
	freeSpace := tablePage.getFreeSpaceForInsert()
	if !deleted {
		th.bufferPoolManager.UnpinPage(rid.PageId, false)
		page.Unlock()
		return false
	}

	headerPage = th.getHeaderPage(true)
	header = createHeapFileHeader(headerPage.Data())
	header.setPageInfo(rid.PageId, pageInfo{
		pageId:    rid.PageId,
		leftSpace: freeSpace,
	})
	th.releaseHeaderPage(headerPage, true)
	
	page.Unlock()
	th.bufferPoolManager.UnpinPage(rid.PageId, true)
	return true
}

func (th *TableHeap) Get(rid common.RID) ([]byte, bool) {
	headerPage := th.getHeaderPage(false)
	header := createHeapFileHeader(headerPage.Data())
	_, ok := header.getPageInfo(rid.PageId)
	th.releaseHeaderPage(headerPage, false)
	if !ok {
		return nil, false
	}

	page, err := th.bufferPoolManager.FetchPage(rid.PageId)
	if err != nil {
		log.WithError(err).Fatalf("Unexpected page not found.")
	}
	page.Lock()
	tablePage := createTablePage(page.Data())
	data, found := tablePage.Get(rid)
	page.Unlock()
	th.bufferPoolManager.UnpinPage(rid.PageId, false)
	return data, found
}
