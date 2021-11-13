package common

import "fmt"

type RID struct {
	PageId  PageId
	SlotNum int
}

func (rid *RID) String() string {
	return fmt.Sprintf("[Page id %d, slot num %d]", rid.PageId, rid.SlotNum)
}
