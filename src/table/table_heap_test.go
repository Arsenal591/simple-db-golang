package table

import (
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"simple-db-golang/src/common"
	"simple-db-golang/src/disk"
)

func TestNewTableHeap(t *testing.T) {
	defer os.Remove("test.db")
	diskManager := disk.NewDiskManager("test.db")
	defer diskManager.Close()
	replacer := disk.NewLRUReplacer()
	bufferPoolManager := disk.NewBufferPoolManager(8, diskManager, replacer)

	tableHeapFile := NewTableHeap(bufferPoolManager, true)

	headerPage := tableHeapFile.getHeaderPage(false)
	header := createHeapFileHeader(headerPage.Data())
	require.Equal(t, int32(0), header.numPages)
	tableHeapFile.releaseHeaderPage(headerPage, false)
}

func testTableDataFunc(t *testing.T, tableHeapFile *TableHeap, allData [][]byte, allRIDs []common.RID) {
	headerPage := tableHeapFile.getHeaderPage(false)
	header := createHeapFileHeader(headerPage.Data())
	pageInfoList := header.getPageInfoList()
	for _, info := range pageInfoList {
		page, _ := tableHeapFile.bufferPoolManager.FetchPage(info.pageId)
		tablePage := createTablePage(page.Data())
		require.Equal(t, info.leftSpace, tablePage.getFreeSpaceForInsert())
		tableHeapFile.bufferPoolManager.UnpinPage(info.pageId, false)
	}

	for i, rid := range allRIDs {
		data, found := tableHeapFile.Get(rid)
		require.True(t, found)
		require.Equal(t, allData[i], data)
	}
}

func insertDeleteUtilsFunc(tableHeapFile *TableHeap, total int, insertProb float64) ([][]byte, []common.RID) {
	allData := make([][]byte, 0)
	allRIDs := make([]common.RID, 0)
	for i := 0; i < total; i++ {
		isInsert := (rand.Float64() <= insertProb) || (len(allRIDs) == 0)
		if isInsert {
			length := rand.Intn(512) + 1
			randStr := make([]byte, length)
			rand.Read(randStr)
			rid := tableHeapFile.Insert(randStr)
			allData = append(allData, randStr)
			allRIDs = append(allRIDs, rid)
		} else { // is delete
			idx := rand.Intn(len(allRIDs))
			tableHeapFile.Delete(allRIDs[idx])

			allData = append(allData[:idx], allData[idx+1:]...)
			allRIDs = append(allRIDs[:idx], allRIDs[idx+1:]...)
		}
	}
	return allData, allRIDs
}

func TestTableHeap_Insert(t *testing.T) {
	defer os.Remove("test.db")

	allData := make([][]byte, 0)
	allRIDs := make([]common.RID, 0)

	diskManager := disk.NewDiskManager("test.db")
	replacer := disk.NewLRUReplacer()
	bufferPoolManager := disk.NewBufferPoolManager(8, diskManager, replacer)
	tableHeapFile := NewTableHeap(bufferPoolManager, true)

	for i := 0; i < 100; i++ {
		length := rand.Intn(512) + 1
		randStr := make([]byte, length)
		rand.Read(randStr)
		rid := tableHeapFile.Insert(randStr)
		allData = append(allData, randStr)
		allRIDs = append(allRIDs, rid)
	}
	testTableDataFunc(t, tableHeapFile, allData, allRIDs)
	bufferPoolManager.FlushAllPages()
	diskManager.Close()

	// Test durability
	secondDiskManager := disk.NewDiskManager("test.db")
	secondReplacer := disk.NewLRUReplacer()
	secondBufferPoolManager := disk.NewBufferPoolManager(8, secondDiskManager, secondReplacer)
	secondTableHeapFile := NewTableHeap(secondBufferPoolManager, false)
	testTableDataFunc(t, secondTableHeapFile, allData, allRIDs)
	secondDiskManager.Close()
}

func TestTableHeap_Insert_Delete_Mixed(t *testing.T) {
	defer os.Remove("test.db")

	diskManager := disk.NewDiskManager("test.db")
	replacer := disk.NewLRUReplacer()
	bufferPoolManager := disk.NewBufferPoolManager(8, diskManager, replacer)
	tableHeapFile := NewTableHeap(bufferPoolManager, true)
	allData, allRIDs := insertDeleteUtilsFunc(tableHeapFile, 100, 0.70)

	testTableDataFunc(t, tableHeapFile, allData, allRIDs)
	bufferPoolManager.FlushAllPages()
	diskManager.Close()

	// Test durability
	secondDiskManager := disk.NewDiskManager("test.db")
	secondReplacer := disk.NewLRUReplacer()
	secondBufferPoolManager := disk.NewBufferPoolManager(8, secondDiskManager, secondReplacer)
	secondTableHeapFile := NewTableHeap(secondBufferPoolManager, false)
	testTableDataFunc(t, secondTableHeapFile, allData, allRIDs)
	secondDiskManager.Close()
}

func TestTableHeap_Insert_Delete_Concurrent(t *testing.T) {
	defer os.Remove("test.db")
	diskManager := disk.NewDiskManager("test.db")
	replacer := disk.NewLRUReplacer()
	bufferPoolManager := disk.NewBufferPoolManager(16, diskManager, replacer)
	tableHeapFile := NewTableHeap(bufferPoolManager, true)

	allData := make([][]byte, 0)
	allRIDs := make([]common.RID, 0)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			partialData, partialRIDs := insertDeleteUtilsFunc(tableHeapFile, 100, 0.7)
			mu.Lock()
			allData = append(allData, partialData...)
			allRIDs = append(allRIDs, partialRIDs...)
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	testTableDataFunc(t, tableHeapFile, allData, allRIDs)
	diskManager.Close()

}
