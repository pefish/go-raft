package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pefish/go-raft/protobuf"
	"io"

	"github.com/gogo/protobuf/proto"
)

// A log entry stores a single item in the log.
type LogEntry struct {
	pb       *protobuf.LogEntry
	Position int64 // position in the log file
	log      *Log
	event    *ev
}

// Creates a new log entry associated with a log.
func newLogEntry(log *Log, event *ev, index uint64, term uint64, command Command) (*LogEntry, error) {
	var buf bytes.Buffer
	var commandName string
	if command != nil {
		commandName = command.CommandName()
		if encoder, ok := command.(CommandEncoder); ok {
			if err := encoder.Encode(&buf); err != nil {
				return nil, err
			}
		} else {
			if err := json.NewEncoder(&buf).Encode(command); err != nil {
				return nil, err
			}
		}
	}

	pb := &protobuf.LogEntry{
		Index:       proto.Uint64(index),
		Term:        proto.Uint64(term),
		CommandName: proto.String(commandName),
		Command:     buf.Bytes(),
	}

	e := &LogEntry{
		pb:    pb,
		log:   log,
		event: event,
	}

	return e, nil
}

func (e *LogEntry) Index() uint64 {
	return e.pb.GetIndex()
}

func (e *LogEntry) Term() uint64 {
	return e.pb.GetTerm()
}

func (e *LogEntry) CommandName() string {
	return e.pb.GetCommandName()
}

func (e *LogEntry) Command() []byte {
	return e.pb.GetCommand()
}

// Encodes the log entry to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (e *LogEntry) Encode(w io.Writer) (int, error) {
	b, err := proto.Marshal(e.pb)
	if err != nil {
		return -1, err
	}

	if _, err = fmt.Fprintf(w, "%8x\n", len(b)); err != nil {
		return -1, err
	}

	return w.Write(b)
}

// Decodes the log entry from a buffer. Returns the number of bytes read and
// any error that occurs.
func (e *LogEntry) Decode(r io.Reader) (int, error) {

	var length int
	_, err := fmt.Fscanf(r, "%8x\n", &length)  // 先从字符串中读出一个4字节数字，表示后面内容的字节数。（"67klkl",读出的就是0x67, "121223233445567"读出的就是0x12122323）
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(r, data)  // 接着读出数据（数据是protobuf格式）

	if err != nil {
		return -1, err
	}

	if err = proto.Unmarshal(data, e.pb); err != nil {  // 解码出数据
		return -1, err
	}

	return length + 8 + 1, nil
}
