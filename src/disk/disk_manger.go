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

type DiskManager struct {
	fileName      string
	header        *headerPageInfo
	headerRawData []byte

	fi *os.File
}

func NewDiskManager(fileName string) *DiskManager {
	fi, err := directio.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0644)
	if err != nil {
		log.WithError(err).Fatalf("Cannot open file.")
	}
	dm := &DiskManager{
		fileName: fileName,
		fi:       fi,
	}
	size, err := dm.getFileSize()
	if err != nil {
		log.WithError(err).Fatalf("Cannot get file size.")
	}
	if size == 0 { // New file
		dm.headerRawData = directio.AlignedBlock(pageSize)
		dm.header = createHeaderPageInfo(dm.headerRawData)
		dm.header.init()
		if err := dm.writeHeaderPage(); err != nil {
			log.WithError(err).Fatalf("Write header page failed.")
		}
	} else {
		dm.headerRawData, err = dm.readPageData(common.PageId(0))
		if err != nil {
			log.WithError(err).Fatalf("Read header page failed.")
		}
		dm.header = createHeaderPageInfo(dm.headerRawData)
	}
	return dm
}

func (dm *DiskManager) Close() error {
	return dm.fi.Close()
}

func (dm *DiskManager) AllocatePage() (common.PageId, []byte) {
	var pageId common.PageId
	var data []byte
	var err error
	if dm.header.hasFreePage() {
		pageId = dm.header.popFreePage()
		data, err = dm.readPageData(pageId)
		if err != nil {
			log.WithError(err).Fatalf("Read page failed.")
		}
	} else {
		pageId = dm.header.nextPageId
		data = directio.AlignedBlock(pageSize)
		if err = dm.writePageData(pageId, data); err != nil {
			log.WithError(err).Fatalf("Create new page failed.")
		}
		dm.header.nextPageId++
	}
	if err = dm.writeHeaderPage(); err != nil {
		log.WithError(err).Fatalf("Write header page failed.")
	}
	return pageId, data
}

func (dm *DiskManager) DeallocatePage(id common.PageId) {
	dm.header.pushFreePage(id)
	if err := dm.writeHeaderPage(); err != nil {
		log.WithError(err).Fatalf("Write header page failed.")
	}
}

func (dm *DiskManager) ReadPage(pageId common.PageId) ([]byte, error) {
	return dm.readPageData(pageId)
}

func (dm *DiskManager) WritePage(pageId common.PageId, data []byte) error {
	return dm.writePageData(pageId, data)
}

func (dm *DiskManager) getFileSize() (int64, error) {
	stat, err := dm.fi.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (dm *DiskManager) readPageData(pageId common.PageId) ([]byte, error) {
	if pageId < 0 {
		return nil, fmt.Errorf("Page id is negative.")
	}
	offset := pageId * pageSize
	size, err := dm.getFileSize()
	if err != nil {
		return nil, err
	}
	if int64(offset) >= size {
		return nil, fmt.Errorf("Read past end of file.")
	}
	if _, err := dm.fi.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	data := directio.AlignedBlock(pageSize)
	if n, err := dm.fi.Read(data); err != nil {
		return nil, err
	} else {
		if n < pageSize {
			return nil, fmt.Errorf("Read less than a page.")
		}
		return data, nil
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
