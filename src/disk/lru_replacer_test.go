package disk

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUReplacer_Add(t *testing.T) {
	replacer := NewLRUReplacer()

	for i := 0; i < 10; i++ {
		replacer.Add(i)
		require.Equal(t, i, replacer.dataList.Front().Value.(int))
		require.Contains(t, replacer.index, i)
	}
}

func TestLRUReplacer_Remove(t *testing.T) {
	replacer := NewLRUReplacer()
	for i := 0; i < 10; i++ {
		replacer.Add(i)
	}

	replacer.Remove(5)
	require.NotContains(t, replacer.index, 5)
	elem4 := replacer.index[4]
	elem6 := replacer.index[6]
	require.Equal(t, elem6.Next(), elem4)
}

func TestLRUReplacer_Victim(t *testing.T) {
	replacer := NewLRUReplacer()
	for i := 0; i < 10; i++ {
		replacer.Add(i)
	}
	for i := 0; i < 10; i++ {
		frameId, ok := replacer.Victim()
		require.Equal(t, true, ok)
		require.Equal(t, i, frameId)
	}
	_, ok := replacer.Victim()
	require.Equal(t, false, ok)
}

func TestLRUReplacer_Hybrid(t *testing.T) {
	replacer := NewLRUReplacer()
	for i := 0; i < 10; i++ {
		replacer.Add(i)
	}
	replacer.Remove(0)
	replacer.Remove(3)
	replacer.Remove(5)

	frameId, ok := replacer.Victim()
	require.Equal(t, true, ok)
	require.Equal(t, 1, frameId)
	frameId, ok = replacer.Victim()
	require.Equal(t, true, ok)
	require.Equal(t, 2, frameId)
	frameId, ok = replacer.Victim()
	require.Equal(t, true, ok)
	require.Equal(t, 4, frameId)

	replacer.Add(5)
	frameId, ok = replacer.Victim()
	require.Equal(t, true, ok)
	require.Equal(t, 6, frameId)
}
