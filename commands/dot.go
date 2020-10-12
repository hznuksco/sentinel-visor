package commands

import (
	"fmt"
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

		db, err := setupStorage(cctx)
		if err != nil {
			return xerrors.Errorf("setup storage and api: %w", err)
		}
		defer func() {
			if err := db.Close(cctx.Context); err != nil {
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
		_, err = db.DB.QueryContext(cctx.Context, &blks, `
			select block, parent, b.miner, b.height, p.height as "parent_height"
			from block_parents
			inner join block_headers b on block_parents.block = b.cid
			inner join block_headers p on block_parents.parent = p.cid
			where b.height >= ? and b.height <= ?`, startHeight, endHeight)
		if err != nil {
			return err
		}

		fmt.Println("digraph D {")
		for _, b := range blks {
			fmt.Println(b.DotString())
		}
		fmt.Println("}")

		return nil
	},
}
