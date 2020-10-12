package blocks

type BlockNode struct {
	Block        string
	Height       uint64
	Parent       string
	ParentHeight uint64
	Miner        string
}
