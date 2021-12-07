package table

import (
	"math"
	"unsafe"

	"simple-db-golang/src/common"
)

type pageInfo struct {
	pageId    common.PageId
	leftSpace int32
}

type heapFileHeader struct {
	numPages int32

	// Pointer of `pageInfo`s.
	ptr struct{}
}

func createHeapFileHeader(data []byte) *heapFileHeader {
	return (*heapFileHeader)(unsafe.Pointer(&data[0]))
}

func (hdr *heapFileHeader) init() {
	hdr.numPages = 0
}

func (hdr *heapFileHeader) getPageInfoList() []pageInfo {
	return (*(*[math.MaxInt32]pageInfo)(unsafe.Pointer(&hdr.ptr)))[:int(hdr.numPages)]
}

func (hdr *heapFileHeader) getPageInfo(pageId common.PageId) (pageInfo, bool) {
	pageInfoList := hdr.getPageInfoList()
	for _, info := range pageInfoList {
		if info.pageId == pageId {
			return info, true
		}
	}
	return pageInfo{}, false
}

func (hdr *heapFileHeader) setPageInfo(pageId common.PageId, info pageInfo) bool {
	pageInfoList := hdr.getPageInfoList()
	for i := 0; i < int(hdr.numPages); i++ {
		if pageInfoList[i].pageId == pageId {
			pageInfoList[i] = info
			return true
		}
	}
	return false
}

func (hdr *heapFileHeader) pushPageInfo(info pageInfo) {
	hdr.numPages += 1
	pageInfoList := hdr.getPageInfoList()
	pageInfoList[int(hdr.numPages)-1] = info
}

func (hdr *heapFileHeader) removePageInfo(pageId common.PageId) bool {
	idx := -1
	pageInfoList := hdr.getPageInfoList()
	for i := 0; i < int(hdr.numPages); i++ {
		if pageInfoList[i].pageId == pageId {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false
	}
	for i := idx; i < int(hdr.numPages)-1; i++ {
		pageInfoList[i] = pageInfoList[i+1]
	}
	hdr.numPages -= 1
	return true
}
