package workers

import (
	ethCommon "github.com/HPISTechnologies/3rd-party/eth/common"
	"github.com/HPISTechnologies/common-lib/types"
	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/aggregator/aggregator"
	"github.com/HPISTechnologies/component-lib/log"
	"go.uber.org/zap"
)

type AggreSelector struct {
	actor.WorkerThread
	aggregator *aggregator.Aggregator
}

//return a Subscriber struct
func NewAggreSelector(concurrency int, groupid string) *AggreSelector {
	agg := AggreSelector{}
	agg.Set(concurrency, groupid)
	agg.aggregator = aggregator.NewAggregator()
	return &agg
}

func (a *AggreSelector) OnStart() {
}

func (a *AggreSelector) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgClearCommand:
			remainingQuantity := a.aggregator.OnClearInfoReceived()
			a.AddLog(log.LogLevel_Info, "generic-hashing AggreSelector clear pool", zap.Int("remainingQuantity", remainingQuantity))
		case actor.MsgInclusive:
			inclusive := msgs[0].Data.(*types.InclusiveList)
			inclusive.Mode = types.InclusiveMode_Message
			result, _ := a.aggregator.OnListReceived(inclusive)
			a.SendMsg(result)
		case actor.MsgReceiptHashList:
			receiptHashLists := v.Data.(*types.ReceiptHashList)
			if receiptHashLists == nil {
				return nil
			}
			for i := range receiptHashLists.TxHashList {
				receiptHash := &types.ReceiptHash{
					Txhash:      &receiptHashLists.TxHashList[i],
					Receipthash: &receiptHashLists.ReceiptHashList[i],
					GasUsed:     receiptHashLists.GasUsedList[i],
				}
				result := a.aggregator.OnDataReceived(receiptHashLists.TxHashList[i], receiptHash)
				a.SendMsg(result)
			}
		}
	}
	return nil
}
func (a *AggreSelector) SendMsg(SelectedData *[]*interface{}) {
	if SelectedData != nil {
		receipts := make(map[ethCommon.Hash]*types.ReceiptHash, len(*SelectedData))
		for _, v := range *SelectedData {
			rh := (*v).(*types.ReceiptHash)
			receipts[*rh.Txhash] = rh
		}
		a.AddLog(log.LogLevel_Info, "AggreSelector send selected receipts")
		a.MsgBroker.Send(actor.MsgSelectedReceipts, &receipts)
	}
}
