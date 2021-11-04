package disk

import (
	"math"
	"unsafe"

	"simple-db-golang/src/common"
)

// todo: use bitmask instead of list of int32
type headerPageInfo struct {
	nextPageId   common.PageId
	numFreePages int32
	freeListPtr  uintptr
}

func createHeaderPageInfo(data []byte) *headerPageInfo {
	return (*headerPageInfo)(unsafe.Pointer(&data[0]))
}

func (hdr *headerPageInfo) init() {
	hdr.nextPageId = 1
	hdr.numFreePages = 0
}

func (hdr *headerPageInfo) get(i int32) common.PageId {
	buf := (*[math.MaxInt32]common.PageId)(unsafe.Pointer(&hdr.freeListPtr))
	return buf[i]
}

func (hdr *headerPageInfo) hasFreePage() bool {
	return hdr.numFreePages > 0
}

func (hdr *headerPageInfo) popFreePage() common.PageId {
	buf := (*[math.MaxInt32]common.PageId)(unsafe.Pointer(&hdr.freeListPtr))
	ret := buf[0]
	for i := int32(1); i < hdr.numFreePages; i++ {
		buf[i-1] = buf[i]
	}
	hdr.numFreePages -= 1
	return ret
}

func (hdr *headerPageInfo) pushFreePage(pageId common.PageId) {
	buf := (*[math.MaxInt32]common.PageId)(unsafe.Pointer(&hdr.freeListPtr))
	buf[hdr.numFreePages] = pageId
	hdr.numFreePages += 1
}
