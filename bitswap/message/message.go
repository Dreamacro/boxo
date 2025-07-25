package message

import (
	"encoding/binary"
	"errors"
	"io"
	"maps"

	"github.com/ipfs/boxo/bitswap/client/wantlist"
	pb "github.com/ipfs/boxo/bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p/core/network"
	msgio "github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"
)

// BitSwapMessage is the basic interface for interacting building, encoding,
// and decoding messages sent on the BitSwap protocol.
type BitSwapMessage interface {
	FillWantlist([]Entry) []Entry
	// Wantlist returns a slice of unique keys that represent data wanted by
	// the sender.
	Wantlist() []Entry

	// Blocks returns a slice of unique blocks.
	Blocks() []blocks.Block
	// BlockPresences returns the list of HAVE / DONT_HAVE in the message
	BlockPresences() []BlockPresence
	// Haves returns the Cids for each HAVE
	Haves() []cid.Cid
	// DontHaves returns the Cids for each DONT_HAVE
	DontHaves() []cid.Cid
	// PendingBytes returns the number of outstanding bytes of data that the
	// engine has yet to send to the client (because they didn't fit in this
	// message)
	PendingBytes() int32

	// AddEntry adds an entry to the Wantlist.
	AddEntry(key cid.Cid, priority int32, wantType pb.Message_Wantlist_WantType, sendDontHave bool) int

	// Cancel adds a CANCEL for the given CID to the message
	// Returns the size of the CANCEL entry in the protobuf
	Cancel(key cid.Cid) int

	// Remove removes any entries for the given CID. Useful when the want
	// status for the CID changes when preparing a message.
	Remove(key cid.Cid)

	// Empty indicates whether the message has any information
	Empty() bool
	// Size returns the size of the message in bytes
	Size() int

	// A full wantlist is an authoritative copy, a 'non-full' wantlist is a patch-set
	Full() bool

	// AddBlock adds a block to the message
	AddBlock(blocks.Block)
	// AddBlockPresence adds a HAVE / DONT_HAVE for the given Cid to the message
	AddBlockPresence(cid.Cid, pb.Message_BlockPresenceType)
	// AddHave adds a HAVE for the given Cid to the message
	AddHave(cid.Cid)
	// AddDontHave adds a DONT_HAVE for the given Cid to the message
	AddDontHave(cid.Cid)
	// SetPendingBytes sets the number of bytes of data that are yet to be sent
	// to the client (because they didn't fit in this message)
	SetPendingBytes(int32)
	Exportable

	Loggable() map[string]interface{}

	// Reset the values in the message back to defaults, so it can be reused
	Reset(bool)

	// Clone the message fields
	Clone() BitSwapMessage
}

// Exportable is an interface for structures than can be
// encoded in a bitswap protobuf.
type Exportable interface {
	// Note that older Bitswap versions use a different wire format, so we need
	// to convert the message to the appropriate format depending on which
	// version of the protocol the remote peer supports.
	ToProtoV0() *pb.Message
	ToProtoV1() *pb.Message
	ToNetV0(w io.Writer) error
	ToNetV1(w io.Writer) error
}

// BlockPresence represents a HAVE / DONT_HAVE for a given Cid
type BlockPresence struct {
	Cid  cid.Cid
	Type pb.Message_BlockPresenceType
}

// Entry is a wantlist entry in a Bitswap message, with flags indicating
// - whether message is a cancel
// - whether requester wants a DONT_HAVE message
// - whether requester wants a HAVE message (instead of the block)
type Entry struct {
	wantlist.Entry
	Cancel       bool
	SendDontHave bool
}

// Get the size of the entry on the wire
func (e *Entry) Size() int {
	return proto.Size(e.ToPB())
}

// Get the entry in protobuf form
func (e *Entry) ToPB() *pb.Message_Wantlist_Entry {
	return &pb.Message_Wantlist_Entry{
		Block:        e.Cid.Bytes(),
		Priority:     e.Priority,
		Cancel:       e.Cancel,
		WantType:     e.WantType,
		SendDontHave: e.SendDontHave,
	}
}

type impl struct {
	full           bool
	wantlist       map[cid.Cid]*Entry
	blocks         map[cid.Cid]blocks.Block
	blockPresences map[cid.Cid]pb.Message_BlockPresenceType
	pendingBytes   int32
}

// New returns a new, empty bitswap message
func New(full bool) *impl {
	return &impl{
		full:           full,
		wantlist:       make(map[cid.Cid]*Entry),
		blocks:         make(map[cid.Cid]blocks.Block),
		blockPresences: make(map[cid.Cid]pb.Message_BlockPresenceType),
	}
}

// Clone the message fields
func (m *impl) Clone() BitSwapMessage {
	return &impl{
		full:           m.full,
		wantlist:       maps.Clone(m.wantlist),
		blocks:         maps.Clone(m.blocks),
		blockPresences: maps.Clone(m.blockPresences),
		pendingBytes:   m.pendingBytes,
	}
}

// Reset the values in the message back to defaults, so it can be reused
func (m *impl) Reset(full bool) {
	m.full = full
	clear(m.wantlist)
	clear(m.blocks)
	clear(m.blockPresences)
	m.pendingBytes = 0
}

var errCidMissing = errors.New("missing cid")

func newMessageFromProto(pbm *pb.Message) (BitSwapMessage, error) {
	m := New(pbm.Wantlist != nil && pbm.Wantlist.Full)

	if pbm.Wantlist != nil {
		for _, e := range pbm.Wantlist.Entries {
			if len(e.Block) == 0 {
				return nil, errCidMissing
			}
			c, err := cid.Cast(e.Block)
			if err != nil {
				return nil, err
			}
			m.addEntry(c, e.Priority, e.Cancel, e.WantType, e.SendDontHave)
		}
	}

	// deprecated
	for _, d := range pbm.Blocks {
		// CIDv0, sha256, protobuf only
		b := blocks.NewBlock(d)
		m.AddBlock(b)
	}
	//

	for _, b := range pbm.GetPayload() {
		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return nil, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return nil, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return nil, err
		}

		m.AddBlock(blk)
	}

	for _, bi := range pbm.GetBlockPresences() {
		if len(bi.Cid) == 0 {
			return nil, errCidMissing
		}
		c, err := cid.Cast(bi.Cid)
		if err != nil {
			return nil, err
		}
		if !c.Defined() {
			return nil, errCidMissing
		}
		m.AddBlockPresence(c, bi.Type)
	}

	m.pendingBytes = pbm.PendingBytes

	return m, nil
}

func (m *impl) Full() bool {
	return m.full
}

func (m *impl) Empty() bool {
	return len(m.blocks) == 0 && len(m.wantlist) == 0 && len(m.blockPresences) == 0
}

func (m *impl) FillWantlist(out []Entry) []Entry {
	if cap(out) < len(m.wantlist) {
		out = make([]Entry, len(m.wantlist))
	} else {
		out = out[:0]
	}
	for _, e := range m.wantlist {
		out = append(out, *e)
	}
	return out
}

func (m *impl) Wantlist() []Entry {
	out := make([]Entry, 0, len(m.wantlist))
	for _, e := range m.wantlist {
		out = append(out, *e)
	}
	return out
}

func (m *impl) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(m.blocks))
	for _, block := range m.blocks {
		bs = append(bs, block)
	}
	return bs
}

func (m *impl) BlockPresences() []BlockPresence {
	bps := make([]BlockPresence, 0, len(m.blockPresences))
	for c, t := range m.blockPresences {
		bps = append(bps, BlockPresence{c, t})
	}
	return bps
}

func (m *impl) Haves() []cid.Cid {
	return m.getBlockPresenceByType(pb.Message_Have)
}

func (m *impl) DontHaves() []cid.Cid {
	return m.getBlockPresenceByType(pb.Message_DontHave)
}

func (m *impl) getBlockPresenceByType(t pb.Message_BlockPresenceType) []cid.Cid {
	cids := make([]cid.Cid, 0, len(m.blockPresences))
	for c, bpt := range m.blockPresences {
		if bpt == t {
			cids = append(cids, c)
		}
	}
	return cids
}

func (m *impl) PendingBytes() int32 {
	return m.pendingBytes
}

func (m *impl) SetPendingBytes(pendingBytes int32) {
	m.pendingBytes = pendingBytes
}

func (m *impl) Remove(k cid.Cid) {
	delete(m.wantlist, k)
}

func (m *impl) Cancel(k cid.Cid) int {
	return m.addEntry(k, 0, true, pb.Message_Wantlist_Block, false)
}

func (m *impl) AddEntry(k cid.Cid, priority int32, wantType pb.Message_Wantlist_WantType, sendDontHave bool) int {
	return m.addEntry(k, priority, false, wantType, sendDontHave)
}

func (m *impl) addEntry(c cid.Cid, priority int32, cancel bool, wantType pb.Message_Wantlist_WantType, sendDontHave bool) int {
	e, exists := m.wantlist[c]
	if exists {
		// Only change priority if want is of the same type
		if e.WantType == wantType {
			e.Priority = priority
		}
		// Only change from "dont cancel" to "do cancel"
		if cancel {
			e.Cancel = cancel
		}
		// Only change from "dont send" to "do send" DONT_HAVE
		if sendDontHave {
			e.SendDontHave = sendDontHave
		}
		// want-block overrides existing want-have
		if wantType == pb.Message_Wantlist_Block && e.WantType == pb.Message_Wantlist_Have {
			e.WantType = wantType
		}
		m.wantlist[c] = e
		return 0
	}

	e = &Entry{
		Entry: wantlist.Entry{
			Cid:      c,
			Priority: priority,
			WantType: wantType,
		},
		SendDontHave: sendDontHave,
		Cancel:       cancel,
	}
	m.wantlist[c] = e

	return e.Size()
}

func (m *impl) AddBlock(b blocks.Block) {
	delete(m.blockPresences, b.Cid())
	m.blocks[b.Cid()] = b
}

func (m *impl) AddBlockPresence(c cid.Cid, t pb.Message_BlockPresenceType) {
	if _, ok := m.blocks[c]; ok {
		return
	}
	m.blockPresences[c] = t
}

func (m *impl) AddHave(c cid.Cid) {
	m.AddBlockPresence(c, pb.Message_Have)
}

func (m *impl) AddDontHave(c cid.Cid) {
	m.AddBlockPresence(c, pb.Message_DontHave)
}

func (m *impl) Size() int {
	size := 0
	for _, block := range m.blocks {
		size += len(block.RawData())
	}
	for c := range m.blockPresences {
		size += BlockPresenceSize(c)
	}
	for _, e := range m.wantlist {
		size += e.Size()
	}

	return size
}

func BlockPresenceSize(c cid.Cid) int {
	return proto.Size(&pb.Message_BlockPresence{
		Cid:  c.Bytes(),
		Type: pb.Message_Have,
	})
}

// FromNet generates a new BitswapMessage from incoming data on an io.Reader.
func FromNet(r io.Reader) (BitSwapMessage, int, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return FromMsgReader(reader)
}

// FromPBReader generates a new Bitswap message from a protobuf reader.
func FromMsgReader(r msgio.Reader) (BitSwapMessage, int, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return nil, 0, err
	}

	pb := new(pb.Message)
	err = proto.Unmarshal(msg, pb)
	r.ReleaseMsg(msg)
	if err != nil {
		return nil, 0, err
	}

	m, err := newMessageFromProto(pb)
	if err != nil {
		return nil, 0, err
	}
	return m, len(msg), nil
}

func (m *impl) ToProtoV0() *pb.Message {
	pbm := &pb.Message{
		Wantlist: &pb.Message_Wantlist{
			Entries: make([]*pb.Message_Wantlist_Entry, 0, len(m.wantlist)),
		},
	}

	for _, e := range m.wantlist {
		pbm.Wantlist.Entries = append(pbm.Wantlist.Entries, e.ToPB())
	}
	pbm.Wantlist.Full = m.full

	blocks := m.Blocks()
	pbm.Blocks = make([][]byte, 0, len(blocks))
	for _, b := range blocks {
		pbm.Blocks = append(pbm.Blocks, b.RawData())
	}
	return pbm
}

func (m *impl) ToProtoV1() *pb.Message {
	pbm := &pb.Message{
		Wantlist: &pb.Message_Wantlist{
			Entries: make([]*pb.Message_Wantlist_Entry, 0, len(m.wantlist)),
		},
	}

	for _, e := range m.wantlist {
		pbm.Wantlist.Entries = append(pbm.Wantlist.Entries, e.ToPB())
	}
	pbm.Wantlist.Full = m.full

	blocks := m.Blocks()
	pbm.Payload = make([]*pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Payload = append(pbm.Payload, &pb.Message_Block{
			Data:   b.RawData(),
			Prefix: b.Cid().Prefix().Bytes(),
		})
	}

	pbm.BlockPresences = make([]*pb.Message_BlockPresence, 0, len(m.blockPresences))
	for c, t := range m.blockPresences {
		pbm.BlockPresences = append(pbm.BlockPresences, &pb.Message_BlockPresence{
			Cid:  c.Bytes(),
			Type: t,
		})
	}

	pbm.PendingBytes = m.PendingBytes()

	return pbm
}

func (m *impl) ToNetV0(w io.Writer) error {
	return write(w, m.ToProtoV0())
}

func (m *impl) ToNetV1(w io.Writer) error {
	return write(w, m.ToProtoV1())
}

func write(w io.Writer, m *pb.Message) error {
	size := proto.Size(m)

	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	opts := proto.MarshalOptions{}
	wrBuf, err := opts.MarshalAppend(buf[:n], m)
	if err != nil {
		return err
	}

	_, err = w.Write(wrBuf)
	return err
}

func (m *impl) Loggable() map[string]interface{} {
	blocks := make([]string, 0, len(m.blocks))
	for _, v := range m.blocks {
		blocks = append(blocks, v.Cid().String())
	}
	return map[string]interface{}{
		"blocks": blocks,
		"wants":  m.Wantlist(),
	}
}
