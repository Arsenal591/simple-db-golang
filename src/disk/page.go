package disk

import (
	"simple-db-golang/src/common"
	"sync"
)

type Page struct {
	data     []byte
	pageId   common.PageId
	pinCount int
	isDirty  bool
	sync.RWMutex
}

func (p *Page) Data() []byte { return p.data }

func (p *Page) PageId() common.PageId { return p.pageId }

func (p *Page) PinCount() int { return p.pinCount }

func (p *Page) IsDirty() bool { return p.isDirty }
