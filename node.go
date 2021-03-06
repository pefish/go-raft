// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"errors"
	pb "github.com/pefish/go-raft/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {  // 软状态，表示易变的状态，有leader信息、本节点的角色状态
	Lead      uint64
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {  // 表示准备要提交的数据（entries以及messages）。应用层会监听ready包，然后进行处理
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState  // 表示本节点的软状态。这里非空就表示Ready包有更新

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState  // 表示本节点的硬状态。这里不是空状态就表示Ready包有更新

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry  // 表示准备要被提交的entries。这里长度大于0就表示Ready包有更新

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot  // 表示待执行快照。这里有值就表示Ready包有更新

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry  // 存储准备要被apply的下一批entries。这里长度大于0就表示Ready包有更新

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message  // 表示上面的Entries被提交后，需要被发送给邻节点的所有消息。这里长度大于0就表示Ready包有更新
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

func (rd Ready) containsUpdates() bool {  // 判断准备处理的包是否包含更新，有更新才被处理，没更新就无需处理。
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	Tick()  // 应用层定时调用这个函数
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log.
	Propose(ctx context.Context, data []byte) error  // 应用层调用这个方法执行命令，命令将被包装成log entry等待共识达成，达成后通过ready通知应用层
	// ProposeConfChange proposes config change.
	// At most one ConfChange can be in the process of going through consensus.
	// Application needs to call ApplyConfChange when applying EntryConfChange type entry.
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error  // 应用层调用这个方法表示邻节点变更，共识完成后，应用层需要调用ApplyConfChange使其生效
	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finish applying the last ready. To make this optimization
	// work safely, when the application receives a Ready with softState.RaftState equal to Candidate
	// it MUST apply all pending configuration changes if there is any.
	//
	// Here is a simple solution that waiting for ALL pending entries to get applied.
	// ```
	// ...
	// rd := <-n.Ready()
	// go apply(rd.CommittedEntries) // optimization to apply asynchronously in FIFO order.
	// if rd.SoftState.RaftState == StateCandidate {
	//     waitAllApplied()
	// }
	// n.Advance()
	// ...
	//```
	Advance()  // 通知raft，应用层已经apply了这个ready包
	// ApplyConfChange applies config change to the local node.
	// Returns an opaque ConfState protobuf which must be recorded
	// in snapshots. Will never return nil; it returns a pointer only
	// to match MemoryStorage.Compact.
	ApplyConfChange(cc pb.ConfChange) *pb.ConfState  // ready包中有EntryConfChange事件时，应用层需要调用这个函数使配置生效
	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot.
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
}

type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
func StartNode(c *Config, peers []Peer) Node {
	r := newRaft(c)  // 新建一个raft实例
	// become the follower at term 1 and apply initial configuration
	// entries of term 1
	r.becomeFollower(1, None)  // 本节点设置为follower，任期设置为1，leader为空
	for _, peer := range peers {  // 针对每个node添加一个EntryConfChange类型的entry，append到本节点log中
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		d, err := cc.Marshal()
		if err != nil {
			panic("unexpected marshal error")
		}
		e := pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: r.raftLog.lastIndex() + 1, Data: d}
		r.raftLog.append(e)
	}
	// Mark these initial entries as committed.
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
	r.raftLog.committed = r.raftLog.lastIndex()  // 设置提交点，标记所有初始化的entries为已提交
	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	for _, peer := range peers {  // 这里立马应用前面的几个EntryConfChange类型的entry，为了测试方便，实际上这几个entry后面还会apply一遍，应为他们被提交了还没有被apply（应用点没有设置）
		r.addNode(peer.ID)
	}

	n := newNode()  // 新建一个node实例
	go n.run(r)
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	r := newRaft(c)

	n := newNode()
	go n.run(r)
	return &n
}

// node is the canonical implementation of the Node interface
type node struct {
	propc      chan pb.Message  // 赞同（propose）数据append的类型消息的通道
	recvc      chan pb.Message  // 除 赞同数据append的类型消息 以外所有类型消息的存放通道
	confc      chan pb.ConfChange  // 配置（这里的配置只是指邻节点的配置）变更消息的通道。由应用层通知配置变更
	confstatec chan pb.ConfState  // 生效的新配置通道。应用层通知配置变更后会监听这个通道来等待节点响应配置变更消息（节点响应完成后会往这里发送配置）
	readyc     chan Ready  // Ready包通道。应用层会监听这个通道
	advancec   chan struct{}  // 通知节点上一个Ready已经处理完毕。由应用层调用node.Advance函数触发
	tickc      chan struct{}
	done       chan struct{}
	stop       chan struct{}  // 监听应用层发起的停止节点的请求
	status     chan chan Status  // 监听应用层发起的获取节点状态的请求
}

func newNode() node {
	return node{
		propc:      make(chan pb.Message),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChange),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		tickc:      make(chan struct{}),
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		status:     make(chan chan Status),
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

func (n *node) run(r *raft) {
	var propc chan pb.Message
	var readyc chan Ready
	var advancec chan struct{}
	var prevLastUnstablei, prevLastUnstablet uint64
	var havePrevLastUnstablei bool  // 上一个ready包中是否存在待提交的entry
	var prevSnapi uint64
	var rd Ready

	lead := None
	prevSoftSt := r.softState()  // 获取本节点的软状态
	prevHardSt := emptyState  // ready处理完会更新这个变量

	for {  // 进入节点死循环
		if advancec != nil {
			readyc = nil
		} else {
			rd = newReady(r, prevSoftSt, prevHardSt)  // 新建一个准备处理的包，软状态给它，表示本节点软状态改变
			if rd.containsUpdates() {  // 判断Ready包是否有更新
				readyc = n.readyc  // 有更新的话，取出ready包通道，下面会往里塞数据
			} else {
				readyc = nil
			}
		}

		if lead != r.lead {  // 如果本节点记录的leader有变化
			if r.hasLeader() {  // 如果存在leader
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case m := <-propc:  // 收到赞同log append类型的消息
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:  // 收到其他类型消息
			// filter out response message from unknown From.
			if _, ok := r.prs[m.From]; ok || !IsResponseMsg(m) {
				r.Step(m) // raft never returns an error
			}
		case cc := <-n.confc:  // 收到配置变更消息（包括添加node、移除node、更新node三种消息）
			if cc.NodeID == None {  // 如果要操作的node id都没有，则什么都不做
				r.resetPendingConf()
				select {
				case n.confstatec <- pb.ConfState{Nodes: r.nodes()}:
				case <-n.done:
				}
				break
			}
			switch cc.Type {
			case pb.ConfChangeAddNode:  // 添加节点
				r.addNode(cc.NodeID)
			case pb.ConfChangeRemoveNode:  // 移除节点
				// block incoming proposal when local node is
				// removed
				if cc.NodeID == r.id {
					n.propc = nil
				}
				r.removeNode(cc.NodeID)
			case pb.ConfChangeUpdateNode:  // 更新节点
				r.resetPendingConf()
			default:
				panic("unexpected conf type")
			}
			select {
			case n.confstatec <- pb.ConfState{Nodes: r.nodes()}:  // 发送新的配置通知
			case <-n.done:
			}
		case <-n.tickc:  // 定时处理。由应用层调用node.Tick函数触发这里
			r.tick()
			r.logger.Debugf("tick... status: %v", r.raftLog.allEntries())
			r.logger.Debugf("tick... committed: %d", r.raftLog.committed)
			r.logger.Debugf("tick... applied: %d", r.raftLog.applied)
			r.logger.Debugf("tick... peers: %d", r.prs)
		case readyc <- rd:  // 向ready包通道塞入ready包，应用层会处理ready包
			if rd.SoftState != nil {
				prevSoftSt = rd.SoftState
			}
			if len(rd.Entries) > 0 {
				prevLastUnstablei = rd.Entries[len(rd.Entries)-1].Index
				prevLastUnstablet = rd.Entries[len(rd.Entries)-1].Term
				havePrevLastUnstablei = true
			}
			if !IsEmptyHardState(rd.HardState) {
				prevHardSt = rd.HardState
			}
			if !IsEmptySnap(rd.Snapshot) {  // 如果存在待执行快照，则取出快照点
				prevSnapi = rd.Snapshot.Metadata.Index
			}
			r.msgs = nil
			advancec = n.advancec  // 开启通道。n.advancec会被应用层通知上一个ready包已处理完
		case <-advancec:  // 上一个ready包处理结束
			if prevHardSt.Commit != 0 {
				r.raftLog.appliedTo(prevHardSt.Commit)  // 设置应用点
			}
			if havePrevLastUnstablei {  // 如果上一个已处理的ready包有待提交的entry，则更新不稳定log
				r.raftLog.stableTo(prevLastUnstablei, prevLastUnstablet)  // 更新不稳定log
				havePrevLastUnstablei = false
			}
			r.raftLog.stableSnapTo(prevSnapi)  // 如果上一个ready包中存在待执行快照，那么将不稳定log中的待执行快照设置为空
			advancec = nil  // 封闭通道
		case c := <-n.status:  // 收到获取本节点状态的请求
			c <- getStatus(r)  // 取出状态放入c通道
		case <-n.stop:
			close(n.done)
			return  // 跳出循环
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m) {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return n.Step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *node) step(ctx context.Context, m pb.Message) error {
	ch := n.recvc
	if m.Type == pb.MsgProp {
		ch = n.propc
	}

	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc:
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)
	n.status <- c
	return <-c
}

func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	return rd
}
