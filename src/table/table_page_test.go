package table

import (
	"testing"

	"simple-db-golang/src/common"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"
)

const (
	pageSize = 4096
)

func initPageByData(page *TablePage, data [][]byte) {
	page.init(common.PageId(1), pageSize)
	insertedDataOffset := make([]int32, 0)
	for i := 0; i < len(data); i++ {
		var prevOffset int32 = pageSize
		if i > 0 {
			prevOffset = insertedDataOffset[i-1]
		}
		insertedDataOffset = append(insertedDataOffset, prevOffset-int32(len(data[i])))
	}
	buf := page.getRecordRawSlice()
	for i := 0; i < len(data); i++ {
		page.pushRecordSlot(RecordSlot{offset: insertedDataOffset[i]})
		copy(buf[int(insertedDataOffset[i]):int(insertedDataOffset[i])+len(data[i])], data[i])
	}
}

func TestTablePage_GetSetSlot(t *testing.T) {
	data := directio.AlignedBlock(pageSize)
	page := createTablePage(data)
	page.init(common.PageId(1), pageSize)

	for i := 0; i < 10; i++ {
		slot := RecordSlot{
			offset: int32(i),
		}
		page.pushRecordSlot(slot)
	}
	for i := 0; i < 10; i++ {
		slot := page.getRecordSlot(i)
		require.Equal(t, int32(i), slot.offset)
	}
	for i := 0; i < 10; i++ {
		slot := RecordSlot{
			offset: int32(10 - i),
		}
		page.setRecordSlot(i, slot)
	}
	for i := 0; i < 10; i++ {
		slot := page.getRecordSlot(i)
		require.Equal(t, int32(10-i), slot.offset)
	}

	pageTwice := createTablePage(data)
	for i := 0; i < 10; i++ {
		slot := pageTwice.getRecordSlot(i)
		require.Equal(t, int32(10-i), slot.offset)
	}
}

func TestTablePage_GetInsertIndex(t *testing.T) {
	data := directio.AlignedBlock(pageSize)
	page := createTablePage(data)
	var index int

	page.init(common.PageId(1), pageSize)
	page.pushRecordSlot(RecordSlot{offset: pageSize - 10})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	index = page.getInsertIndex()
	require.Equal(t, 2, index) // No available space. Insert at end.

	page.init(common.PageId(1), pageSize)
	page.pushRecordSlot(RecordSlot{offset: pageSize})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 10})
	index = page.getInsertIndex()
	require.Equal(t, 0, index) // First record is empty. Insert at 0.

	page.init(common.PageId(1), pageSize)
	page.pushRecordSlot(RecordSlot{offset: pageSize - 10})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 30})
	index = page.getInsertIndex()
	require.Equal(t, 2, index) // Record 2 is empty. Insert at 2.

	page.init(common.PageId(1), pageSize)
	page.pushRecordSlot(RecordSlot{offset: pageSize - 10})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 30})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 30})
	index = page.getInsertIndex()
	require.Equal(t, 2, index) // Multiple records are empty. Use the first one.

	page.init(common.PageId(1), pageSize)
	page.pushRecordSlot(RecordSlot{offset: pageSize - 10})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 20})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 30})
	page.pushRecordSlot(RecordSlot{offset: pageSize - 30})
	index = page.getInsertIndex()
	require.Equal(t, 3, index) // Last record is empty.

}

func TestTablePage_MoveBackRecords(t *testing.T) {
	data := directio.AlignedBlock(pageSize)
	page := createTablePage(data)

	insertedData := [][]byte{[]byte("hello"), []byte("world"), []byte("alice")}
	insertedDataOffset := make([]int32, 0)
	for i := 0; i < len(insertedData); i++ {
		var prevOffset int32 = pageSize
		if i > 0 {
			prevOffset = insertedDataOffset[i-1]
		}
		insertedDataOffset = append(insertedDataOffset, prevOffset-int32(len(insertedData[i])))
	}
	testCases := []struct {
		startIndex          int
		size                int
		expectedStartOffset int32
	}{
		{
			startIndex:          0,
			size:                4,
			expectedStartOffset: insertedDataOffset[0] - 4,
		},
		{
			startIndex:          1,
			size:                4,
			expectedStartOffset: insertedDataOffset[1] - 4,
		},
		{
			startIndex:          2,
			size:                4,
			expectedStartOffset: insertedDataOffset[2] - 4,
		},
	}

	for _, tc := range testCases {
		page.init(common.PageId(1), pageSize)
		buf := page.getRecordRawSlice()
		for i := 0; i < len(insertedData); i++ {
			page.pushRecordSlot(RecordSlot{offset: insertedDataOffset[i]})
			copy(buf[int(insertedDataOffset[i]):int(insertedDataOffset[i])+len(insertedData[i])], insertedData[i])
		}
		startOffset := page.moveBackRecords(tc.startIndex, tc.size)
		require.Equal(t, tc.expectedStartOffset, int32(startOffset))

		// Check whether record data and pointers are moved correctly.
		for i := 0; i < len(insertedData); i++ {
			offset := page.getRecordOffset(i)
			if i <= tc.startIndex {
				require.Equal(t, insertedDataOffset[i], offset)
			} else {
				require.Equal(t, insertedDataOffset[i]-int32(tc.size), offset)
			}
			readData := buf[int(offset) : int(offset)+len(insertedData[i])]
			require.Equal(t, insertedData[i], readData)
		}
	}
}

func TestTablePage_Insert(t *testing.T) {
	data := directio.AlignedBlock(pageSize)
	page := createTablePage(data)

	testCases := []struct {
		originData   [][]byte
		insertedData []byte
		expectedData [][]byte
		expectedRID  common.RID
	}{
		{
			originData:   [][]byte{[]byte("hello"), []byte("world"), []byte("alice")},
			insertedData: []byte("bob"),
			expectedData: [][]byte{[]byte("hello"), []byte("world"), []byte("alice"), []byte("bob")},
			expectedRID:  common.RID{PageId: common.PageId(1), SlotNum: 3},
		},
		{
			originData:   [][]byte{[]byte(""), []byte("world"), []byte("alice")},
			insertedData: []byte("bob"),
			expectedData: [][]byte{[]byte("bob"), []byte("world"), []byte("alice")},
			expectedRID:  common.RID{PageId: common.PageId(1), SlotNum: 0},
		},
		{
			originData:   [][]byte{[]byte("hello"), []byte(""), []byte("alice")},
			insertedData: []byte("bob"),
			expectedData: [][]byte{[]byte("hello"), []byte("bob"), []byte("alice")},
			expectedRID:  common.RID{PageId: common.PageId(1), SlotNum: 1},
		},
		{
			originData:   [][]byte{[]byte("hello"), []byte("world"), []byte("")},
			insertedData: []byte("bob"),
			expectedData: [][]byte{[]byte("hello"), []byte("world"), []byte("bob")},
			expectedRID:  common.RID{PageId: common.PageId(1), SlotNum: 2},
		},
		{
			originData:   [][]byte{[]byte("hello"), []byte("world"), []byte(""), []byte(""), []byte("alice")},
			insertedData: []byte("bob"),
			expectedData: [][]byte{[]byte("hello"), []byte("world"), []byte("bob"), []byte(""), []byte("alice")},
			expectedRID:  common.RID{PageId: common.PageId(1), SlotNum: 2},
		},
	}

	for _, tc := range testCases {
		initPageByData(page, tc.originData)

		rid, _ := page.Insert(tc.insertedData)

		require.Equal(t, tc.expectedRID, rid)
		require.Equal(t, len(tc.expectedData), int(page.numRecords))
		for i := 0; i < int(page.numRecords); i++ {
			readData := page.getRecord(i)
			require.Equal(t, tc.expectedData[i], readData)
		}
	}
}

func TestTablePage_Get(t *testing.T) {
	data := directio.AlignedBlock(pageSize)
	page := createTablePage(data)

	insertedData := [][]byte{[]byte("hello"), []byte("world"), []byte(""), []byte("alice")}
	initPageByData(page, insertedData)

	testCases := []struct {
		rid          common.RID
		expectedData []byte
		expectedOk   bool
	}{
		{
			rid:          common.RID{PageId: common.PageId(1), SlotNum: 0},
			expectedData: []byte("hello"),
			expectedOk:   true,
		},
		{
			rid:          common.RID{PageId: common.PageId(1), SlotNum: 2},
			expectedData: nil,
			expectedOk:   false,
		},
		{
			rid:          common.RID{PageId: common.PageId(1), SlotNum: 4},
			expectedData: nil,
			expectedOk:   false,
		},
	}

	for _, tc := range testCases {
		readData, ok := page.Get(tc.rid)
		require.Equal(t, tc.expectedOk, ok)
		require.Equal(t, tc.expectedData, readData)
	}
}

func TestTablePage_Delete(t *testing.T) {
	data := directio.AlignedBlock(pageSize)
	page := createTablePage(data)

	testCases := []struct {
		originData     [][]byte
		deletedRID     common.RID
		expectedData   [][]byte
		expectedResult bool
	}{
		{
			originData:     [][]byte{[]byte("hello"), []byte("world"), []byte("alice")},
			deletedRID:     common.RID{PageId: common.PageId(1), SlotNum: 1},
			expectedData:   [][]byte{[]byte("hello"), []byte(""), []byte("alice")},
			expectedResult: true,
		},
		{
			originData:     [][]byte{[]byte("hello"), []byte("world"), []byte("alice")},
			deletedRID:     common.RID{PageId: common.PageId(1), SlotNum: 3},
			expectedData:   [][]byte{[]byte("hello"), []byte("world"), []byte("alice")},
			expectedResult: false,
		},
		{
			originData:     [][]byte{[]byte("hello"), []byte(""), []byte("alice")},
			deletedRID:     common.RID{PageId: common.PageId(1), SlotNum: 1},
			expectedData:   [][]byte{[]byte("hello"), []byte(""), []byte("alice")},
			expectedResult: false,
		},
	}

	for _, tc := range testCases {
		initPageByData(page, tc.originData)
		ok := page.Delete(tc.deletedRID)
		require.Equal(t, tc.expectedResult, ok)
		require.Equal(t, len(tc.expectedData), int(page.numRecords))
		for i := 0; i < int(page.numRecords); i++ {
			readData := page.getRecord(i)
			require.Equal(t, tc.expectedData[i], readData)
		}
	}
}
