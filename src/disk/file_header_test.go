package disk

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"simple-db-golang/src/common"
)

func TestUnderlyingRawData(t *testing.T) {
	data := make([]byte, pageSize)
	first_hdr := createHeaderPageInfo(data)

	for i := 0; i < 50; i++ {
		num := rand.Intn(3)
		if num == 0 {
			pageId := rand.Intn(1 << 16)
			first_hdr.pushFreePage(common.PageId(pageId))
		} else if num == 1 {
			if first_hdr.hasFreePage() {
				first_hdr.popFreePage()
			}
		} else {
			first_hdr.nextPageId = common.PageId(rand.Intn(1 << 16))
		}
	}

	second_hdr := createHeaderPageInfo(data)
	require.Equal(t, first_hdr.nextPageId, second_hdr.nextPageId)
	require.Equal(t, first_hdr.numFreePages, second_hdr.numFreePages)

	for i := int32(0); i < first_hdr.numFreePages; i++ {
		require.Equal(t, first_hdr.get(i), second_hdr.get(i))
	}
}

func TestPushFreePage(t *testing.T) {
	data := make([]byte, pageSize)
	hdr := createHeaderPageInfo(data)
	hdr.init()

	for i := 0; i < 10; i++ {
		hdr.pushFreePage(common.PageId(i))
	}
	require.Equal(t, int32(10), hdr.numFreePages)
	for i := 0; i < 10; i++ {
		require.Equal(t, common.PageId(i), hdr.get(int32(i)))
	}
}

func TestPopFreePage(t *testing.T) {
	data := make([]byte, pageSize)
	hdr := createHeaderPageInfo(data)
	hdr.init()

	for i := 0; i < 10; i++ {
		hdr.pushFreePage(common.PageId(i))
	}
	for i := 0; i < 10; i++ {
		require.Equal(t, common.PageId(i), hdr.popFreePage())
	}
}
