package disk

import (
	"fmt"
	"io"
	"os"

	"github.com/ncw/directio"
	log "github.com/sirupsen/logrus"

	"simple-db-golang/src/common"
)

const (
	pageSize = 4096
)

// todo: We simply assume that there will be only one header page in each file.
type DiskManager struct {
	fileName      string
	header        *headerPageInfo
	headerRawData []byte

	fi          *os.File
	freePageSet map[common.PageId]struct{}
}

func NewDiskManager(fileName string) *DiskManager {
	fi, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
	if err != nil {
		log.WithError(err).Fatalf("Cannot open file.")
	}
	dm := &DiskManager{
		fileName:      fileName,
		fi:            fi,
		headerRawData: directio.AlignedBlock(pageSize),
		freePageSet:   make(map[common.PageId]struct{}),
	}
	size, err := dm.getFileSize()
	if err != nil {
		log.WithError(err).Fatalf("Cannot get file size.")
	}
	if size == 0 { // New file
		dm.header = createHeaderPageInfo(dm.headerRawData)
		dm.header.init()
		if err := dm.writeHeaderPage(); err != nil {
			log.WithError(err).Fatalf("Write header page failed.")
		}
	} else {
		err = dm.readPageData(common.PageId(0), dm.headerRawData)
		if err != nil {
			log.WithError(err).Fatalf("Read header page failed.")
		}
		dm.header = createHeaderPageInfo(dm.headerRawData)

		for i := int32(0); i < dm.header.numFreePages; i++ {
			freePageId := dm.header.get(i)
			dm.freePageSet[freePageId] = struct{}{}
		}
	}
	return dm
}

func (dm *DiskManager) Close() error {
	return dm.fi.Close()
}

func (dm *DiskManager) AllocatePage() (common.PageId, error) {
	var pageId common.PageId
	var data []byte
	var err error
	if dm.header.hasFreePage() {
		pageId = dm.header.popFreePage()
		delete(dm.freePageSet, pageId)
	} else {
		pageId = dm.header.nextPageId
		data = directio.AlignedBlock(pageSize)
		if err = dm.writePageData(pageId, data); err != nil {
			log.WithError(err).Errorf("Create new page failed.")
			return 0, err
		}
		dm.header.nextPageId++
	}
	if err = dm.writeHeaderPage(); err != nil {
		log.WithError(err).Fatalf("Write header page failed.")
	}
	return pageId, nil
}

func (dm *DiskManager) DeallocatePage(id common.PageId) error {
	if id >= dm.header.nextPageId {
		return fmt.Errorf("Page %d is not in the file.", id)
	}
	if _, ok := dm.freePageSet[id]; ok {
		return fmt.Errorf("Page %d is already deallocated.", id)
	}
	dm.freePageSet[id] = struct{}{}
	dm.header.pushFreePage(id)
	if err := dm.writeHeaderPage(); err != nil {
		log.WithError(err).Fatalf("Write header page failed.")
	}
	return nil
}

func (dm *DiskManager) ReadPage(pageId common.PageId, data []byte) error {
	if pageId >= dm.header.nextPageId {
		return fmt.Errorf("Page %d is not in the file.", pageId)
	}
	if _, ok := dm.freePageSet[pageId]; ok {
		return fmt.Errorf("Page %d is already deallocated.", pageId)
	}
	return dm.readPageData(pageId, data)
}

func (dm *DiskManager) WritePage(pageId common.PageId, data []byte) error {
	if pageId >= dm.header.nextPageId {
		return fmt.Errorf("Page %d is not in the file.", pageId)
	}
	if _, ok := dm.freePageSet[pageId]; ok {
		return fmt.Errorf("Page %d is already deallocated.", pageId)
	}
	return dm.writePageData(pageId, data)
}

func (dm *DiskManager) getFileSize() (int64, error) {
	stat, err := dm.fi.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (dm *DiskManager) readPageData(pageId common.PageId, data []byte) error {
	if pageId < 0 {
		return fmt.Errorf("Page id is negative.")
	}
	offset := pageId * pageSize
	size, err := dm.getFileSize()
	if err != nil {
		return err
	}
	if int64(offset) >= size {
		return fmt.Errorf("Read past end of file.")
	}
	if _, err := dm.fi.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	if n, err := dm.fi.Read(data); err != nil {
		return err
	} else {
		if n < pageSize {
			return fmt.Errorf("Read less than a page.")
		}
		return nil
	}
}

func (dm *DiskManager) writePageData(pageId common.PageId, data []byte) error {
	if pageId < 0 {
		return fmt.Errorf("Page id is negative.")
	}
	offset := pageId * pageSize
	if _, err := dm.fi.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	if _, err := dm.fi.Write(data); err != nil {
		return err
	}
	return nil
}

func (dm *DiskManager) writeHeaderPage() error {
	return dm.writePageData(common.PageId(0), dm.headerRawData)
}
