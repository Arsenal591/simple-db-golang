package table

import (
	"math"
	"unsafe"

	"simple-db-golang/src/common"
)

type TablePage struct {
	pageId     common.PageId
	pageSize   int32
	numRecords int32
	ptr        struct{}
}

type RecordSlot struct {
	offset int32
}

const (
	RecordSlotSize = int(unsafe.Sizeof(RecordSlot{}))
)

func createTablePage(data []byte) *TablePage {
	return (*TablePage)(unsafe.Pointer(&data[0]))
}

func (tp *TablePage) init(pageId common.PageId, pageSize int32) {
	tp.pageId = pageId
	tp.pageSize = pageSize
	tp.numRecords = 0
}

func (tp *TablePage) getSlotSlice() []RecordSlot {
	return (*(*[math.MaxInt32]RecordSlot)(unsafe.Pointer(&tp.ptr)))[:int(tp.numRecords)]
}

func (tp *TablePage) getRecordRawSlice() []byte {
	return (*[math.MaxInt32]byte)(unsafe.Pointer(tp))[:int(tp.pageSize)]
}

func (tp *TablePage) getRecordOffset(i int) int32 {
	slots := tp.getSlotSlice()
	return slots[i].offset
}

func (tp *TablePage) getRecordSlot(i int) RecordSlot {
	slots := tp.getSlotSlice()
	return slots[i]
}

func (tp *TablePage) setRecordSlot(i int, slot RecordSlot) {
	slots := tp.getSlotSlice()
	slots[i] = slot
}

func (tp *TablePage) getRecordSize(i int) int32 {
	offset := tp.getRecordOffset(i)
	endOffset := tp.pageSize
	if i > 0 {
		endOffset = tp.getRecordOffset(i - 1)
	}
	return endOffset - offset
}

func (tp *TablePage) pushRecordSlot(slot RecordSlot) {
	tp.numRecords += 1
	tp.setRecordSlot(int(tp.numRecords)-1, slot)
}

func (tp *TablePage) getRecordStartOffset() int32 {
	startOffset := tp.pageSize
	if int(tp.numRecords) >= 1 {
		startOffset = tp.getRecordOffset(int(tp.numRecords) - 1)
	}
	return startOffset
}

func (tp *TablePage) getFreeSpace() int32 {
	fixedHeaderSize := int32(unsafe.Offsetof(tp.ptr))
	pointerListSize := int32(RecordSlotSize) * tp.numRecords
	startOffset := tp.getRecordStartOffset()
	return startOffset - (fixedHeaderSize + pointerListSize)
}

func (tp *TablePage) getFreeSpaceForInsert() int32 {
	return tp.getFreeSpace() - int32(RecordSlotSize)
}

func (tp *TablePage) getInsertIndex() int {
	prevRecordOffset := tp.pageSize
	for i := 0; i < int(tp.numRecords); i++ {
		offset := tp.getRecordOffset(i)
		if offset == prevRecordOffset {
			return i
		}
		prevRecordOffset = offset
	}
	return int(tp.numRecords)
}

// Move all index > startIndex records back, in order to make space for new data.
// Return value is the start offset of the new allocated space.
func (tp *TablePage) moveBackRecords(startIndex int, size int) int {
	if startIndex == int(tp.numRecords) {
		return int(tp.getRecordStartOffset()) - size
	}
	copyStartOffset := tp.getRecordStartOffset()
	copyEndOffset := tp.getRecordOffset(startIndex)
	if copyStartOffset != copyEndOffset {
		buf := tp.getRecordRawSlice()
		copy(buf[int(copyStartOffset)-size:int(copyEndOffset)-size], buf[int(copyStartOffset):int(copyEndOffset)])
	}

	for i := startIndex + 1; i < int(tp.numRecords); i++ {
		slot := tp.getRecordSlot(i)
		slot.offset -= int32(size)
		tp.setRecordSlot(i, slot)
	}
	return int(copyEndOffset) - size
}

func (tp *TablePage) Insert(record []byte) (common.RID, bool) {
	freeSpace := tp.getFreeSpace()
	if freeSpace < int32(RecordSlotSize+len(record)) {
		return common.RID{}, false
	}
	recordLen := len(record)

	// Try to find a slot that contains no data.
	index := tp.getInsertIndex()

	// Allocate space for the record.
	newRecordStartOffset := tp.moveBackRecords(index, recordLen)

	// Insert binary data.
	buf := tp.getRecordRawSlice()
	copy(buf[newRecordStartOffset:newRecordStartOffset+recordLen], record)

	// Update pointers.
	if index == int(tp.numRecords) {
		tp.pushRecordSlot(RecordSlot{offset: int32(newRecordStartOffset)})
	} else {
		tp.setRecordSlot(index, RecordSlot{offset: int32(newRecordStartOffset)})
	}
	return common.RID{
		PageId:  tp.pageId,
		SlotNum: index,
	}, true
}

func (tp *TablePage) Delete(rid common.RID) bool {
	if rid.SlotNum >= int(tp.numRecords) {
		return false
	}
	size := tp.getRecordSize(rid.SlotNum)
	if size == 0 { // previously deleted

		return false
	}
	tp.moveBackRecords(rid.SlotNum, -int(size))

	// Update pointers
	slot := tp.getRecordSlot(rid.SlotNum)
	slot.offset += size
	tp.setRecordSlot(rid.SlotNum, slot)
	return true
}

func (tp *TablePage) getRecord(i int) []byte {
	offset := tp.getRecordOffset(i)
	endOffset := tp.pageSize
	if i > 0 {
		endOffset = tp.getRecordOffset(i - 1)
	}
	buf := tp.getRecordRawSlice()
	return buf[offset:endOffset]
}

func (tp *TablePage) Get(rid common.RID) ([]byte, bool) {
	if rid.SlotNum >= int(tp.numRecords) {
		return nil, false
	}
	data := tp.getRecord(rid.SlotNum)
	if len(data) == 0 {
		return nil, false
	}
	ret := make([]byte, len(data), len(data))
	copy(ret, data)
	return ret, true
}
