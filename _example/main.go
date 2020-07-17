package main

import (
	"context"
	"fmt"
	"github.com/pefish/go-raft"
	pb "github.com/pefish/go-raft/raftpb"
	"log"
	"os"
	"time"
)

func process(entry pb.Entry)    { // 应用已提交的entry
	fmt.Println("process", entry)
}

func sendMessages(msgs []pb.Message)  { // 消息发送给所有邻节点
	fmt.Println("sendMessages", msgs)
}

func processSnapshot(st pb.Snapshot) {  // 处理快照
	fmt.Println("processSnapshot", st)
}

func saveToStorage(hardState pb.HardState, entries []pb.Entry, snapshot pb.Snapshot)      { // ready包中的数据都保存起来，保存数据库或者任何地方
	fmt.Println("saveToStorage", hardState, entries, snapshot)
}

func main() {
	var myId uint64 = 1

	raftStorage := raft.NewMemoryStorage()
	d := &raft.DefaultLogger{Logger: log.New(os.Stderr, "raft", log.LstdFlags)}
	d.EnableDebug()
	logger := raft.Logger(d)
	c := &raft.Config{
		ID: 1,
		ElectionTick:              3,
		HeartbeatTick:             1,
		Storage:                   raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		Logger: logger,
	}

	n := raft.StartNode(c, []raft.Peer{
		{
			ID: myId,
		},
	})
	defer n.Stop()

	go func() {
		t := time.NewTicker(2 * time.Second)
		t1 := time.NewTicker(20 * time.Second)
		for {
			select {
			case <- t.C:
				err := n.Propose(context.Background(), []byte("test"))
				if err != nil {
					log.Fatal(err)
				}
				case <- t1.C:
					// raft 不负责节点关系维护以及节点交互，只负责共识
					err := n.ProposeConfChange(context.Background(), pb.ConfChange{
						ID:               myId,
						Type:             pb.ConfChangeAddNode,
						NodeID:           1,
						Context:          []byte("haha"),
						XXX_unrecognized: nil,
					})
					if err != nil {
						log.Fatal(err)
					}

			}

		}
	}()


	t := time.NewTimer(0)
	for {
		select {
		case <- t.C:
			fmt.Println("--------------------- next tick ---------------------")
			n.Tick()
			t.Reset(time.Second)
		case rd := <-n.Ready():
			saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			sendMessages(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				process(entry)
				if entry.Type == pb.EntryConfChange {
					var cc pb.ConfChange
					err := cc.Unmarshal(entry.Data)
					if err != nil {
						log.Fatal(err)
					}
					n.ApplyConfChange(cc)
				}
			}
			n.Advance()
		}

	}
}
