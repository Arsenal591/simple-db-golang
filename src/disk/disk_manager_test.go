package disk

import (
	"math/rand"
	"os"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"

	"simple-db-golang/src/common"
)

var testFileName = "tmp-file"

func TestNewDiskManager(t *testing.T) {
	defer os.Remove(testFileName)
	dm := NewDiskManager(testFileName)
	defer dm.Close()

	require.Equal(t, testFileName, dm.fileName)
	require.Equal(t, int32(0), dm.header.numFreePages)
	require.Equal(t, common.PageId(1), dm.header.nextPageId)

	// Check whether the header page is written.
	fi, _ := os.Open(testFileName)
	defer fi.Close()
	headerPageData := directio.AlignedBlock(pageSize)
	n, err := fi.Read(headerPageData)
	require.Nil(t, err)
	require.Equal(t, pageSize, n)
	expectedHeader := createHeaderPageInfo(headerPageData)
	require.Equal(t, int32(0), expectedHeader.numFreePages)
	require.Equal(t, common.PageId(1), expectedHeader.nextPageId)
	require.Equal(t, 0, len(dm.freePageSet))
}

func TestReadWrite(t *testing.T) {
	defer os.Remove(testFileName)
	dm := NewDiskManager(testFileName)

	all_data := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		pageId, err := dm.AllocatePage()
		require.Nil(t, err)
		data := directio.AlignedBlock(pageSize)
		rand.Read(data)
		all_data = append(all_data, data)
		dm.WritePage(pageId, data)
		secondData := directio.AlignedBlock(pageSize)
		err = dm.ReadPage(pageId, secondData)
		require.Nil(t, err)
		require.Equal(t, data, secondData)
	}
	dm.Close()

	new_dm := NewDiskManager(testFileName)
	defer new_dm.Close()
	for i := 0; i < 10; i++ {
		data := directio.AlignedBlock(pageSize)
		err := new_dm.ReadPage(common.PageId(i+1), data)
		require.Nil(t, err)
		require.Equal(t, all_data[i], data)
	}
}

func TestAllocateAndDeallocate(t *testing.T) {
	defer os.Remove(testFileName)
	dm := NewDiskManager(testFileName)
	defer dm.Close()

	// Allocate pages in sequence.
	for i := 1; i <= 5; i++ {
		pageId, err := dm.AllocatePage()
		require.Nil(t, err)
		require.Equal(t, common.PageId(i), pageId)
		require.Equal(t, common.PageId(i+1), dm.header.nextPageId)
		require.Equal(t, int32(0), dm.header.numFreePages)
	}

	// Deallocate pages in sequence.
	for i := 1; i <= 5; i++ {
		dm.DeallocatePage(common.PageId(i))
		require.Equal(t, common.PageId(6), dm.header.nextPageId)
		require.Equal(t, int32(i), dm.header.numFreePages)
		require.Equal(t, common.PageId(i), dm.header.get(int32(i-1)))
		require.Contains(t, dm.freePageSet, common.PageId(i))
	}

	// Allocate some pages, then deallocate some, finally allocate again.
	for i := 1; i <= 5; i++ {
		dm.AllocatePage()
	}
	for i := 1; i <= 3; i++ {
		dm.DeallocatePage(common.PageId(i))
	}
	for i := 1; i <= 3; i++ {
		pageId, err := dm.AllocatePage()
		require.Nil(t, err)
		require.Equal(t, common.PageId(i), pageId)
		require.Equal(t, common.PageId(6), dm.header.nextPageId)
		require.Equal(t, int32(3-i), dm.header.numFreePages)
		require.NotContains(t, dm.freePageSet, common.PageId(i))
	}
	for i := 1; i <= 5; i++ {
		pageId, err := dm.AllocatePage()
		require.Nil(t, err)
		require.Equal(t, common.PageId(i+5), pageId)
		require.Equal(t, common.PageId(i+6), dm.header.nextPageId)
		require.Equal(t, int32(0), dm.header.numFreePages)
	}
}

func TestDiskManager_ReadWriteAfterDeallocate(t *testing.T) {
	defer os.Remove(testFileName)
	dm := NewDiskManager(testFileName)
	defer dm.Close()

	for i := 1; i <= 5; i++ {
		pageId, _ := dm.AllocatePage()
		data := directio.AlignedBlock(pageSize)
		rand.Read(data)
		dm.WritePage(pageId, data)
	}
	dm.DeallocatePage(common.PageId(1))

	pageId, _ := dm.AllocatePage()
	data := directio.AlignedBlock(pageSize)
	rand.Read(data)
	dm.WritePage(pageId, data)
	secondData := directio.AlignedBlock(pageSize)
	dm.ReadPage(pageId, secondData)
	require.Equal(t, data, secondData)

}

func TestDiskManager_DeallocatePageTwice(t *testing.T) {
	defer os.Remove(testFileName)
	dm := NewDiskManager(testFileName)
	defer dm.Close()

	pageId, _ := dm.AllocatePage()
	dm.DeallocatePage(pageId)
	err := dm.DeallocatePage(pageId)
	require.NotNil(t, err)
}

func TestHeaderPage(t *testing.T) {
	defer os.Remove(testFileName)
	dm := NewDiskManager(testFileName)

	for i := 0; i < 5; i++ {
		dm.AllocatePage()
	}
	dm.DeallocatePage(common.PageId(2))
	dm.DeallocatePage(common.PageId(4))
	dm.Close()

	new_dm := NewDiskManager(testFileName)
	defer new_dm.Close()

	require.Equal(t, int32(2), new_dm.header.numFreePages)
	require.Equal(t, common.PageId(6), new_dm.header.nextPageId)
	require.Equal(t, common.PageId(2), new_dm.header.get(int32(0)))
	require.Equal(t, common.PageId(4), new_dm.header.get(int32(1)))
	require.Equal(t, 2, len(new_dm.freePageSet))
	require.Contains(t, new_dm.freePageSet, common.PageId(2))
	require.Contains(t, new_dm.freePageSet, common.PageId(4))
}
