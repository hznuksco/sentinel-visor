package commands

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/model/blocks"
)

var Dot = &cli.Command{
	Name:      "dot",
	Usage:     "Generate dot graphs for persisted blockchain starting from <minHeight> and includes the following <chainDistance> tipsets",
	ArgsUsage: "<startHeight> <chainDistance>",
	Action: func(cctx *cli.Context) error {
		if err := setupLogging(cctx); err != nil {
			return xerrors.Errorf("setup logging: %w", err)
		}

		ctx := context.TODO()
		db, err := setupStorage(cctx)
		if err != nil {
			return xerrors.Errorf("setup storage and api: %w", err)
		}
		defer func() {
			if err := db.Close(ctx); err != nil {
				log.Errorw("close database", "error", err)
			}
		}()

		startHeight, err := strconv.ParseInt(cctx.Args().Get(0), 10, 32)
		if err != nil {
			return err
		}
		desiredChainLen, err := strconv.ParseInt(cctx.Args().Get(1), 10, 32)
		if err != nil {
			return err
		}
		endHeight := startHeight + desiredChainLen

		var blks = make([]*blocks.BlockNode, desiredChainLen)
		_, err = db.DB.QueryContext(ctx, &blks, `
			select block, parent, b.miner, b.height, p.height as "parent_height"
			from block_parents
			inner join block_headers b on block_parents.block = b.cid
			inner join block_headers p on block_parents.parent = p.cid
			where b.height >= ? and b.height <= ?`, startHeight, endHeight)
		if err != nil {
			return err
		}

		//d, err := blocks.Dot(os.Stdout)
		//if err != nil {
		//return err
		//}

		fmt.Println("digraph D {")

		for _, b := range blks {
			//bc, err := cid.Parse(b.Block)
			//if err != nil {
			//return err
			//}

			//_, has := hl[bc]

			col := crc32.Checksum([]byte(b.Miner), crc32.MakeTable(crc32.Castagnoli))&0xc0c0c0c0 + 0x30303030

			hasstr := ""
			//if !has {
			////col = 0xffffffff
			//hasstr = " UNSYNCED"
			//}

			nulls := b.Height - b.ParentHeight - 1
			for i := uint64(0); i < nulls; i++ {
				name := b.Block + "NP" + fmt.Sprint(i)

				fmt.Printf("%s [label = \"NULL:%d\", fillcolor = \"#ffddff\", style=filled, forcelabels=true]\n%s -> %s\n",
					name, b.Height-nulls+i, name, b.Parent)

				b.Parent = name
			}

			fmt.Printf("%s [label = \"%s:%d%s\", fillcolor = \"#%06x\", style=filled, forcelabels=true]\n%s -> %s\n", b.Block, b.Miner, b.Height, hasstr, col, b.Block, b.Parent)
		}
		fmt.Println("}")

		return nil
	},
}
