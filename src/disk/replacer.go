package disk

type Replacer interface {
	Victim() (int, bool)
	Add(int)
	Remove(int)
	Size() int
}
