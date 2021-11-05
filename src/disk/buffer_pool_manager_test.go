package disk

import (
	"math/rand"
	"os"
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/require"

	"simple-db-golang/src/common"
)

var (
	tmpFileName = "tmp-file"
)

func TestNewBufferPoolManager(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	require.Equal(t, 0, len(bfm.pageTable))
	require.Equal(t, 4, len(bfm.pages))
	require.Equal(t, 4, bfm.size)
	require.Equal(t, 4, bfm.freeList.Len())
}

func TestBufferPoolManager_NewPage(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	for i := 0; i < 4; i++ {
		page, _ := bfm.NewPage()
		require.NotNil(t, page)
		require.Equal(t, common.PageId(i+1), page.pageId)
		require.Equal(t, 1, page.pinCount)
		require.Equal(t, false, page.isDirty)

		require.Equal(t, i+1, len(bfm.pageTable))
		require.Equal(t, 3-i, bfm.freeList.Len())
		require.Equal(t, 0, bfm.replacer.Size())
	}
	page, _ := bfm.NewPage()
	require.Nil(t, page) // Is full.
}

func TestBufferPoolManager_UnpinPage(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	bfm.NewPage() // allocate page 1
	bfm.NewPage() // allocate page 2

	bfm.UnpinPage(common.PageId(2), false)
	require.Equal(t, 2, len(bfm.pageTable))
	require.Equal(t, 2, bfm.freeList.Len())
	require.Equal(t, 1, bfm.replacer.Size())
	require.Equal(t, false, bfm.pages[bfm.pageTable[common.PageId(2)]].isDirty)
	require.Equal(t, 0, bfm.pages[bfm.pageTable[common.PageId(2)]].pinCount)

	bfm.UnpinPage(common.PageId(1), true)
	require.Equal(t, 2, len(bfm.pageTable))
	require.Equal(t, 2, bfm.freeList.Len())
	require.Equal(t, 2, bfm.replacer.Size())
	require.Equal(t, true, bfm.pages[bfm.pageTable[common.PageId(1)]].isDirty)
	require.Equal(t, 0, bfm.pages[bfm.pageTable[common.PageId(1)]].pinCount)
}

func TestBufferPoolManager_FetchPage(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	bfm.NewPage() // allocate page 1
	bfm.NewPage() // allocate page 2

	page, _ := bfm.FetchPage(common.PageId(1))
	require.NotNil(t, page)
	require.Equal(t, 2, page.pinCount)

	bfm.UnpinPage(common.PageId(2), false)

	page, _ = bfm.FetchPage(common.PageId(2))
	require.NotNil(t, page)
	require.Equal(t, 1, page.pinCount)
}

func TestBufferPoolManager_DeletePage(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	bfm.NewPage() // allocate page 1
	bfm.NewPage() // allocate page 2

	err := bfm.DeletePage(common.PageId(1))
	require.NotNil(t, err) // The page is still pinned.
	bfm.UnpinPage(common.PageId(1), false)
	err = bfm.DeletePage(common.PageId(1))
	require.Nil(t, err)
	require.Equal(t, 3, bfm.freeList.Len())
}

func TestBufferPoolManager_Full(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	for i := 0; i < 4; i++ {
		bfm.NewPage()
	}
	for i := 0; i < 4; i++ {
		bfm.UnpinPage(common.PageId(i+1), false)
	}
	bfm.NewPage()
	bfm.UnpinPage(common.PageId(5), false)

	for i := 0; i < 4; i++ {
		_, err := bfm.FetchPage(common.PageId(i + 1))
		require.Nil(t, err)
	}
	page, _ := bfm.NewPage()
	require.Nil(t, page)
	page, _ = bfm.FetchPage(common.PageId(5))
	require.Nil(t, page)
}

func TestBufferPoolManager_FetchPageVictim(t *testing.T) {
	defer os.Remove(tmpFileName)
	dm := NewDiskManager(tmpFileName)
	defer dm.Close()
	lru := NewLRUReplacer()
	bfm := NewBufferPoolManager(4, dm, lru)

	bfm.NewPage() // allocate page 1
	bfm.NewPage() // allocate page 2
	bfm.NewPage()
	require.Equal(t, 2, bfm.pageTable[common.PageId(3)]) // from free list
	bfm.NewPage()
	require.Equal(t, 3, bfm.pageTable[common.PageId(4)]) // from free list

	bfm.UnpinPage(common.PageId(1), true)
	bfm.UnpinPage(common.PageId(2), true)
	bfm.NewPage()
	require.Equal(t, 0, bfm.pageTable[common.PageId(5)]) // from unpinned page

	bfm.UnpinPage(common.PageId(3), true)
	bfm.UnpinPage(common.PageId(4), true)
	bfm.DeletePage(common.PageId(3))
	bfm.FetchPage(common.PageId(1))
	require.Equal(t, 2, bfm.pageTable[common.PageId(1)]) // from free list, use pages 3's space.
}

func TestBufferPoolManager_BinaryData(t *testing.T) {
	defer os.Remove(tmpFileName)
	allDatas := make([][]byte, 0)
	{
		dm := NewDiskManager(tmpFileName)
		defer dm.Close()
		lru := NewLRUReplacer()
		bfm := NewBufferPoolManager(4, dm, lru)

		for i := 0; i < 10; i++ {
			page, _ := bfm.NewPage()
			rand.Read(page.Data())
			copyData := directio.AlignedBlock(pageSize)
			copy(copyData, page.Data())
			allDatas = append(allDatas, copyData)
			bfm.UnpinPage(page.PageId(), true)
		}
		for i := 0; i < 10; i++ {
			page, _ := bfm.FetchPage(common.PageId(i + 1))
			require.Equal(t, allDatas[i], page.Data())
			bfm.UnpinPage(page.PageId(), false)
		}
		bfm.FlushAllPages()
	}
	{
		// open the file again, check if data persists
		dm := NewDiskManager(tmpFileName)
		defer dm.Close()
		lru := NewLRUReplacer()
		bfm := NewBufferPoolManager(4, dm, lru)

		for i := 0; i < 10; i++ {
			page, _ := bfm.FetchPage(common.PageId(i + 1))
			require.Equal(t, allDatas[i], page.Data())
			bfm.UnpinPage(page.PageId(), false)
		}
	}
}
