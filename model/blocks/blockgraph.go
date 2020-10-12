package blocks

import (
	"fmt"
	"hash/crc32"
	"strings"
)

type BlockNode struct {
	Block        string
	Height       uint64
	Parent       string
	ParentHeight uint64
	Miner        string
}

var defaultTbl = crc32.MakeTable(crc32.Castagnoli)

func (b *BlockNode) DotString() string {
	var result strings.Builder

	// write any null rounds before this block
	nulls := b.Height - b.ParentHeight - 1
	for i := uint64(0); i < nulls; i++ {
		name := b.Block + "NP" + fmt.Sprint(i)
		result.WriteString(fmt.Sprintf("%s [label = \"NULL:%d\", fillcolor = \"#ffddff\", style=filled, forcelabels=true]\n%s -> %s\n", name, b.Height-nulls+i, name, b.Parent))
		b.Parent = name
	}

	result.WriteString(fmt.Sprintf("%s [label = \"%s:%d\", fillcolor = \"#%06x\", style=filled, forcelabels=true]\n%s -> %s\n", b.Block, b.Miner, b.Height, b.DotColor(), b.Block, b.Parent))

	return result.String()
}

func (b *BlockNode) DotColor() uint32 {
	return crc32.Checksum([]byte(b.Miner), defaultTbl)&0xc0c0c0c0 + 0x30303030
}
