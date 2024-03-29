package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	if d.RaftGroup.HasReady() {

		rd := d.RaftGroup.Ready()

		// 处理 raftlog 有关的事情
		ret, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			return
		}
		if ret != nil {
			d.peerStorage.SetRegion(ret.Region)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
			d.ctx.storeMeta.Unlock()
		}

		// 处理 raft message 的相关事情
		if len(rd.Messages) > 0 {
			d.Send(d.ctx.trans, rd.Messages)
		}

		// 如果有已经 commit 的事务
		if len(rd.CommittedEntries) > 0 {

			d.HandleCommitEntries(rd)
		}

		d.RaftGroup.Advance(rd)
	}

}

// replyProposals 尝试将 entry 之前的 proposal 全部进行回复
func (d *peerMsgHandler) replyProposals(resp *raft_cmdpb.RaftCmdResponse, entry *eraftpb.Entry, updateTx bool) {

	for len(d.proposals) > 0 {
		p := d.proposals[0]

		if p.index < entry.Index {

			p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			d.proposals = d.proposals[1:]

		} else if p.index == entry.Index {
			// 要求 index 和 term 全部都相等
			if p.term != entry.Term {
				p.cb.Done(ErrRespStaleCommand(entry.Term))
			} else {
				if updateTx {
					p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}
				p.cb.Done(resp)
			}
			d.proposals = d.proposals[1:]
			break
		}

	}

}

// HandleCommitEntries 尝试将所有 commited entries 中的 request 执行，并且将该 entry 写入到 raft 持久化中。
func (d *peerMsgHandler) HandleCommitEntries(rd raft.Ready) {

	for _, ent := range rd.CommittedEntries {

		kvWB := new(engine_util.WriteBatch)

		// 这里 EntryType_EntryConfChange 类型不能够直接被 marshal
		if ent.EntryType == eraftpb.EntryType_EntryConfChange {
			d.execConfChangeRequest(&ent, kvWB)
		} else {
			d.execCommittedRequests(&ent, kvWB)
		}

		d.peerStorage.applyState.AppliedIndex = ent.Index
		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		if err != nil {
			panic(err)
		}

		if d.stopped {
			WB := new(engine_util.WriteBatch)
			WB.DeleteMeta(meta.ApplyStateKey(d.regionId))
			err = WB.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				panic(err)
			}
			return
		}
		//kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		if err != nil {
			panic(err)
		}
	}

}

func (d *peerMsgHandler) findPeer(peerId uint64) int {
	for i, p := range d.Region().Peers {
		if p.Id == peerId {
			return i
		}
	}
	return -1
}

func (d *peerMsgHandler) execConfChangeRequest(ent *eraftpb.Entry, kvWB *engine_util.WriteBatch) {

	cc := eraftpb.ConfChange{}
	err := cc.Unmarshal(ent.Data)
	if err != nil {
		log.Panicf("Invalid ConfChange Entry %+v ,reason: %s", ent, err.Error())
	}
	msg := &raft_cmdpb.RaftCmdRequest{}
	err = msg.Unmarshal(cc.Context)
	if err != nil {
		log.Panicf("Invalid RaftCmdRequest %+v ,reason: %s", ent, err.Error())
	}

	// remove this node
	if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() {

		// 如果节点数量是 2，并且该节点为 leader，那么需要转移 leader 所属权
		if len(d.Region().Peers) == 2 && d.peer.RaftGroup.Raft.State == raft.StateLeader {

			var otherId uint64
			if d.Region().Peers[0].Id == d.PeerId() {
				otherId = d.Region().Peers[1].Id
			} else {
				otherId = d.Region().Peers[0].Id
			}

			// 将控制权转移给另外一个节点
			d.RaftGroup.TransferLeader(otherId)
			log.Infof("Transfer leader instead of remove node")
			return
		}

		d.destroyPeer()
		kvWB.DeleteMeta(meta.ApplyStateKey(d.regionId))
		return
	}

	p := msg.AdminRequest.ChangePeer.Peer

	pos := d.findPeer(cc.NodeId)

	// there is no action to do
	if (cc.ChangeType == eraftpb.ConfChangeType_AddNode && pos != -1) || (cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && pos == -1) {

		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{},
		}
		d.replyProposals(resp, ent, false)
		return
	}

	// revise change in application layer
	region := d.Region()
	region.RegionEpoch.ConfVer++

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {

		// this node is to be added
		region.Peers = append(region.Peers, p)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		d.insertPeerCache(p)

	} else {

		region.Peers = append(region.Peers[:pos], region.Peers[pos+1:]...)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		d.removePeerCache(cc.NodeId)
	}

	d.SetRegion(region)
	// apply conf change in raft layer
	d.RaftGroup.ApplyConfChange(cc)
	// to speed up refresh cache
	//	d.notifyHeartbeatScheduler(region, d.peer)

	// reply to
	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{},
	}
	d.replyProposals(resp, ent, false)
}

// execCommittedRequests 尝试执行 entry 之前的全部 proposal。
// kvWB 这个参数是因为，request 是被包含在一条 log entry 中的，在写 log entry 时也需要把 request 也写到同一个操作中。
func (d *peerMsgHandler) execCommittedRequests(ent *eraftpb.Entry, kvWB *engine_util.WriteBatch) {

	// 这一个函数的逻辑，先执行 entry 中所有的 request，然后对 proposals 进行提交和回复
	// 当一个 entry 提交后，它并没有被写入到 kv 数据库里面，因此还需要进行一次写入操作。
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(ent.Data)
	if err != nil {
		log.Panicf("Invalid Entry %+v , reason: %s", ent, err.Error())
	}

	if len(msg.Requests) == 0 {
		if msg.AdminRequest != nil {
			switch msg.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_InvalidAdmin:

			case raft_cmdpb.AdminCmdType_ChangePeer:

			case raft_cmdpb.AdminCmdType_CompactLog:

				compactLog := msg.AdminRequest.GetCompactLog()
				if compactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
					d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
					d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
					_ = kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					d.ScheduleCompactLog(compactLog.CompactIndex)
				}

			case raft_cmdpb.AdminCmdType_TransferLeader:

			case raft_cmdpb.AdminCmdType_Split:
				req := msg.AdminRequest.Split

				// do some check
				if msg.Header.RegionId != d.regionId {
					regionNotFound := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
					resp := ErrResp(regionNotFound)
					d.replyProposals(resp, ent, false)
					return
				}

				err = util.CheckKeyInRegion(req.SplitKey, d.Region())
				if err != nil {
					//d.replyProposals(ErrResp(err), ent, false)
					d.replyProposals(ErrRespStaleCommand(d.Term()), ent, false)
					return
				}

				// create new region
				var rightRegion metapb.Region
				if err := util.CloneMsg(d.Region(), &rightRegion); err != nil {
					return
				}

				prs := make([]*metapb.Peer, 0, len(req.NewPeerIds))
				for i := 0; i < len(d.Region().Peers); i++ {
					for j := 0; j < len(d.Region().Peers)-i-1; j++ {
						if d.Region().Peers[j].Id > d.Region().Peers[j+1].Id {
							temp := d.Region().Peers[j+1]
							d.Region().Peers[j+1] = d.Region().Peers[j]
							d.Region().Peers[j] = temp
						}
					}
				}
				for i, peer := range d.Region().Peers {
					prs = append(prs, &metapb.Peer{Id: req.NewPeerIds[i], StoreId: peer.StoreId})
				}

				//
				rightRegion.Id = req.NewRegionId
				rightRegion.RegionEpoch.Version++
				rightRegion.StartKey = req.SplitKey
				rightRegion.EndKey = d.Region().EndKey
				rightRegion.Peers = prs

				// create new peer and register
				newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, &rightRegion)
				if err != nil {
					return
				}

				d.Region().EndKey = req.SplitKey
				d.Region().RegionEpoch.Version++
				// persist two regions
				meta.WriteRegionState(kvWB, &rightRegion, rspb.PeerState_Normal)
				meta.WriteRegionState(kvWB, d.Region(), rspb.PeerState_Normal)
				d.ctx.storeMeta.Lock()

				// update ranges
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
				d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: &rightRegion})
				d.ctx.storeMeta.regions[req.NewRegionId] = &rightRegion
				d.ctx.storeMeta.Unlock()

				// start up peer and register
				d.ctx.router.register(newPeer)
				_ = d.ctx.router.send(req.NewRegionId,
					message.Msg{RegionID: req.NewRegionId, Type: message.MsgTypeStart})

				resp := &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
					AdminResponse: &raft_cmdpb.AdminResponse{
						CmdType: raft_cmdpb.AdminCmdType_Split,
						Split: &raft_cmdpb.SplitResponse{
							Regions: []*metapb.Region{&rightRegion, d.Region()},
						},
					},
				}
				d.replyProposals(resp, ent, false)

				d.notifyHeartbeatScheduler(d.Region(), d.peer)
				d.notifyHeartbeatScheduler(&rightRegion, newPeer)

			}

		}
		return
	}

	if len(msg.Requests) > 0 {
		req := msg.Requests[0]
		// applying
		if req.CmdType == raft_cmdpb.CmdType_Put {

			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)

		} else if req.CmdType == raft_cmdpb.CmdType_Delete {

			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
		}

		// callback after applying
		if len(d.proposals) > 0 {
			p := d.proposals[0]

			for p.index < ent.Index {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				d.proposals = d.proposals[1:]
				if len(d.proposals) == 0 {
					return
				}
				p = d.proposals[0]
			}
			if p.index == ent.Index {
				if p.term != ent.Term {
					NotifyStaleReq(ent.Term, p.cb)
				} else {
					resp := newCmdResp()

					switch req.CmdType {
					case raft_cmdpb.CmdType_Get:
						value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get,
							Get: &raft_cmdpb.GetResponse{Value: value}}}

					case raft_cmdpb.CmdType_Put:
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put,
							Put: &raft_cmdpb.PutResponse{}}}

					case raft_cmdpb.CmdType_Delete:
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete,
							Delete: &raft_cmdpb.DeleteResponse{}}}

					case raft_cmdpb.CmdType_Snap:
						if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
							p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
							return
						}
						resp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap,
							Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
						p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
					}
					p.cb.Done(resp)
				}
				d.proposals = d.proposals[1:]
			}
		}
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// proposeRaftCommand 只是将请求发送到 raft 集群中，但是不会直接写入
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).

	if msg.AdminRequest != nil {

		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			//msg.AdminRequest.CompactLog.CompactIndex
			//msg.AdminRequest.CompactLog.CompactTerm
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			_ = d.RaftGroup.Propose(data)

		case raft_cmdpb.AdminCmdType_ChangePeer:

			context, _ := msg.Marshal()
			cc := eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    context,
			}
			p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
			err = d.RaftGroup.ProposeConfChange(cc)
			if err != nil {
				p.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				return
			}
			d.proposals = append(d.proposals, p)

		case raft_cmdpb.AdminCmdType_TransferLeader:

			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			resp := newCmdResp()
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(resp)

		case raft_cmdpb.AdminCmdType_Split:

			if msg.Header.RegionId != d.regionId {
				regionNotFound := &util.ErrRegionNotFound{RegionId: msg.Header.RegionId}
				resp := ErrResp(regionNotFound)
				cb.Done(resp)
				break
			}
			// check the key is in current region
			err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, d.Region())
			if err != nil {
				cb.Done(ErrRespStaleCommand(d.Term()))
				break
				//cb.Done(ErrResp(err))
			}
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}

			// add proposal
			p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
			d.proposals = append(d.proposals, p)
			_ = d.RaftGroup.Propose(data)

		default:
			log.Panicf("Invalid admin request type: %s", raft_cmdpb.AdminCmdType_name[int32(msg.AdminRequest.CmdType)])
		}
	}

	// 每一次取第一条消息进行处理
	for len(msg.Requests) > 0 {
		req := msg.Requests[0]
		var key []byte

		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		default:
		}
		err = util.CheckKeyInRegion(key, d.Region())
		if err != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
			cb.Done(ErrResp(err))
			msg.Requests = msg.Requests[1:]
			continue
		}
		data, err := msg.Marshal()
		if err != nil && data != nil {
			panic(err)
		}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)

		_ = d.RaftGroup.Propose(data)

		msg.Requests = msg.Requests[1:]
	}

}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}
