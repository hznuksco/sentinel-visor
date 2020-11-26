package chain

import (
	"context"
	"math"
	"math/big"

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/model"
	derivedmodel "github.com/filecoin-project/sentinel-visor/model/derived"
	messagemodel "github.com/filecoin-project/sentinel-visor/model/messages"
	visormodel "github.com/filecoin-project/sentinel-visor/model/visor"
)

const MessagesTask = "messages"

type MessageProcessor struct {
	node       lens.API
	opener     lens.APIOpener
	closer     lens.APICloser
	lastTipSet *types.TipSet
}

func NewMessageProcessor(opener lens.APIOpener) *MessageProcessor {
	return &MessageProcessor{
		opener: opener,
	}
}

func (p *MessageProcessor) ProcessTipSet(ctx context.Context, ts *types.TipSet) (model.PersistableWithTx, *visormodel.ProcessingReport, error) {
	if p.node == nil {
		node, closer, err := p.opener.Open(ctx)
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to open lens: %w", err)
		}
		p.node = node
		p.closer = closer
	}

	var data model.PersistableWithTx
	var report *visormodel.ProcessingReport
	var err error

	if p.lastTipSet != nil {
		if p.lastTipSet.Height() > ts.Height() {
			// last tipset seen was the child
			data, report, err = p.processExecutedMessages(ctx, p.lastTipSet, ts)
		} else if p.lastTipSet.Height() < ts.Height() {
			// last tipset seen was the parent
			data, report, err = p.processExecutedMessages(ctx, ts, p.lastTipSet)
		} else {
			// TODO: record in database that we were unable to process messages for this tipset
			log.Errorw("out of order tipsets", "height", ts.Height(), "last_height", p.lastTipSet.Height())
		}
	}

	p.lastTipSet = ts

	// TODO: close lens if rpc error
	return data, report, err
}

// Note that all this processing is in the context of the parent tipset. The child is only used for receipts
func (p *MessageProcessor) processExecutedMessages(ctx context.Context, ts, pts *types.TipSet) (model.PersistableWithTx, *visormodel.ProcessingReport, error) {
	report := &visormodel.ProcessingReport{
		Height:    int64(ts.Height()),
		Task:      ActorStateTask,
		StateRoot: pts.ParentState().String(),
	}

	emsgs, err := p.node.GetExecutedMessageForTipset(ctx, ts, pts)
	if err != nil {
		report.ErrorsDetected = xerrors.Errorf("failed to get executed messages: %w", err)
		return nil, report, nil
	}

	var (
		messageResults       = make(messagemodel.Messages, 0, len(emsgs))
		blockMessageResults  = make(messagemodel.BlockMessages, 0, len(emsgs))
		parsedMessageResults = make(messagemodel.ParsedMessages, 0, len(emsgs))
		gasOutputsResults    = make(derivedmodel.GasOutputsList, 0, len(emsgs))
		errorsDetected       = make([]*MessageError, 0, len(emsgs))
	)

	var (
		seen = make(map[cid.Cid]bool, len(emsgs))

		totalGasLimit     int64
		totalUniqGasLimit int64
	)

	for _, m := range emsgs {
		// Stop processing if we have been told to cancel
		select {
		case <-ctx.Done():
			return nil, nil, xerrors.Errorf("context done: %w", ctx.Err())
		default:
		}

		// Record which blocks had which messages, regardless of duplicates
		blockMessageResults = append(blockMessageResults, &messagemodel.BlockMessage{
			Height:  int64(m.Height),
			Block:   m.BlockCid.String(),
			Message: m.Cid.String(),
		})

		totalGasLimit += m.Msg.GasLimit

		if seen[m.Cid] {
			continue
		}
		seen[m.Cid] = true
		totalUniqGasLimit += m.Msg.GasLimit

		var msgSize int
		if b, err := m.Msg.Serialize(); err == nil {
			msgSize = len(b)
		} else {
			errorsDetected = append(errorsDetected, &MessageError{
				Cid:   m.Cid,
				Error: xerrors.Errorf("failed to serialize message: %w", err).Error(),
			})
		}

		// record all unique messages
		msg := &messagemodel.Message{
			Height:     int64(m.Height),
			Cid:        m.Cid.String(),
			From:       m.Msg.From.String(),
			To:         m.Msg.To.String(),
			Value:      m.Msg.Value.String(),
			GasFeeCap:  m.Msg.GasFeeCap.String(),
			GasPremium: m.Msg.GasPremium.String(),
			GasLimit:   m.Msg.GasLimit,
			SizeBytes:  msgSize,
			Nonce:      m.Msg.Nonce,
			Method:     uint64(m.Msg.Method),
		}
		messageResults = append(messageResults, msg)

		outputs := p.node.ComputeGasOutputs(m.Rcpt.GasUsed, m.Msg.GasLimit, m.Block.ParentBaseFee, m.Msg.GasFeeCap, m.Msg.GasPremium)
		gasOutput := &derivedmodel.GasOutputs{
			Cid:           m.Msg.Cid().String(),
			From:          m.Msg.From.String(),
			To:            m.Msg.To.String(),
			Value:         m.Msg.Value.String(),
			GasFeeCap:     m.Msg.GasFeeCap.String(),
			GasPremium:    m.Msg.GasPremium.String(),
			GasLimit:      m.Msg.GasLimit,
			Nonce:         m.Msg.Nonce,
			Method:        uint64(m.Msg.Method),
			StateRoot:     m.Block.ParentStateRoot.String(),
			ExitCode:      int64(m.Rcpt.ExitCode),
			GasUsed:       m.Rcpt.GasUsed,
			ParentBaseFee: m.Block.ParentBaseFee.String(),

			// TODO: is SizeBytes really needed here?
			SizeBytes:          msgSize,
			BaseFeeBurn:        outputs.BaseFeeBurn.String(),
			OverEstimationBurn: outputs.OverEstimationBurn.String(),
			MinerPenalty:       outputs.MinerPenalty.String(),
			MinerTip:           outputs.MinerTip.String(),
			Refund:             outputs.Refund.String(),
			GasRefund:          outputs.GasRefund,
			GasBurned:          outputs.GasBurned,
		}
		gasOutputsResults = append(gasOutputsResults, gasOutput)

		// TODO: parsed messages
		_ = parsedMessageResults
	}

	newBaseFee := store.ComputeNextBaseFee(pts.Blocks()[0].ParentBaseFee, totalUniqGasLimit, len(pts.Blocks()), pts.Height())
	baseFeeRat := new(big.Rat).SetFrac(newBaseFee.Int, new(big.Int).SetUint64(build.FilecoinPrecision))
	baseFee, _ := baseFeeRat.Float64()

	baseFeeChange := new(big.Rat).SetFrac(newBaseFee.Int, ts.Blocks()[0].ParentBaseFee.Int)
	baseFeeChangeF, _ := baseFeeChange.Float64()

	messageGasEconomyResult := &messagemodel.MessageGasEconomy{
		Height:              int64(pts.Height()),
		StateRoot:           pts.ParentState().String(),
		GasLimitTotal:       totalGasLimit,
		GasLimitUniqueTotal: totalUniqGasLimit,
		BaseFee:             baseFee,
		BaseFeeChangeLog:    math.Log(baseFeeChangeF) / math.Log(1.125),
		GasFillRatio:        float64(totalGasLimit) / float64(len(ts.Blocks())*build.BlockGasTarget),
		GasCapacityRatio:    float64(totalUniqGasLimit) / float64(len(ts.Blocks())*build.BlockGasTarget),
		GasWasteRatio:       float64(totalGasLimit-totalUniqGasLimit) / float64(len(ts.Blocks())*build.BlockGasTarget),
	}

	if len(errorsDetected) != 0 {
		report.ErrorsDetected = errorsDetected
	}

	return PersistableWithTxList{
		messageResults,
		blockMessageResults,
		parsedMessageResults,
		gasOutputsResults,
		messageGasEconomyResult,
	}, report, nil
}

func (p *MessageProcessor) Close() error {
	if p.closer != nil {
		p.closer()
	}
	return nil
}

type MessageError struct {
	Cid   cid.Cid
	Error string
}
