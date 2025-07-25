package bsnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	bsmsg "github.com/ipfs/boxo/bitswap/message"
	iface "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/network/bsnet/internal"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multistream"
)

var log = logging.Logger("bitswap/bsnet")

var (
	maxSendTimeout = 2 * time.Minute
	minSendTimeout = 10 * time.Second
	sendLatency    = 2 * time.Second
	minSendRate    = (100 * 1000) / 8 // 100kbit/s
)

// NewFromIpfsHost returns a BitSwapNetwork supported by underlying IPFS host.
func NewFromIpfsHost(host host.Host, opts ...NetOpt) iface.BitSwapNetwork {
	s := processSettings(opts...)

	bitswapNetwork := impl{
		host: host,

		protocolBitswapNoVers:  s.ProtocolPrefix + ProtocolBitswapNoVers,
		protocolBitswapOneZero: s.ProtocolPrefix + ProtocolBitswapOneZero,
		protocolBitswapOneOne:  s.ProtocolPrefix + ProtocolBitswapOneOne,
		protocolBitswap:        s.ProtocolPrefix + ProtocolBitswap,

		supportedProtocols: s.SupportedProtocols,

		metrics: newMetrics(),
	}

	return &bitswapNetwork
}

func processSettings(opts ...NetOpt) Settings {
	s := Settings{SupportedProtocols: append([]protocol.ID(nil), internal.DefaultProtocols...)}
	for _, opt := range opts {
		opt(&s)
	}
	for i, proto := range s.SupportedProtocols {
		s.SupportedProtocols[i] = s.ProtocolPrefix + proto
	}
	return s
}

// impl transforms the ipfs network interface, which sends and receives
// NetMessage objects, into the bitswap network interface.
type impl struct {
	// NOTE: Stats must be at the top of the heap allocation to ensure 64bit
	// alignment.
	stats iface.Stats

	host          host.Host
	connectEvtMgr *iface.ConnectEventManager

	protocolBitswapNoVers  protocol.ID
	protocolBitswapOneZero protocol.ID
	protocolBitswapOneOne  protocol.ID
	protocolBitswap        protocol.ID

	supportedProtocols []protocol.ID

	// inbound messages from the network are forwarded to the receiver
	receivers []iface.Receiver

	metrics *metrics
}

// interfaceWrapper is concrete type that wraps an interface. Necessary because
// atomic.Value needs the same type and can not Store(nil). This indirection
// allows us to store nil.
type interfaceWrapper[T any] struct {
	t T
}
type atomicInterface[T any] struct {
	iface atomic.Value
}

func (a *atomicInterface[T]) Load() T {
	var v T
	x := a.iface.Load()
	if x != nil {
		return x.(interfaceWrapper[T]).t
	}
	return v
}

func (a *atomicInterface[T]) Store(v T) {
	a.iface.Store(interfaceWrapper[T]{v})
}

type streamMessageSender struct {
	to     peer.ID
	stream atomicInterface[network.Stream]
	bsnet  *impl
	opts   *iface.MessageSenderOpts
}

type HasContext interface {
	Context() context.Context
}

// Open a stream to the remote peer
func (s *streamMessageSender) Connect(ctx context.Context) (network.Stream, error) {
	stream := s.stream.Load()
	if stream != nil {
		return stream, nil
	}

	tctx, cancel := context.WithTimeout(ctx, s.opts.SendTimeout)
	defer cancel()

	if err := s.bsnet.Connect(ctx, peer.AddrInfo{ID: s.to}); err != nil {
		return nil, err
	}

	stream, err := s.bsnet.newStreamToPeer(tctx, s.to)
	if err != nil {
		return nil, err
	}
	if withCtx, ok := stream.Conn().(HasContext); ok {
		context.AfterFunc(withCtx.Context(), func() {
			s.stream.Store(nil)
		})
	}

	s.stream.Store(stream)
	return stream, nil
}

// Reset the stream
func (s *streamMessageSender) Reset() error {
	stream := s.stream.Load()
	if stream != nil {
		err := stream.Reset()
		s.stream.Store(nil)
		return err
	}
	return nil
}

// Indicates whether the peer supports HAVE / DONT_HAVE messages
func (s *streamMessageSender) SupportsHave() bool {
	stream := s.stream.Load()
	if stream == nil {
		return false
	}
	return s.bsnet.SupportsHave(stream.Protocol())
}

// Send a message to the peer, attempting multiple times
func (s *streamMessageSender) SendMsg(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	if n := len(msg.Wantlist()); n > 0 {
		s.bsnet.metrics.WantlistsTotal.Inc()
		s.bsnet.metrics.WantlistsItemsTotal.Add(float64(n))
		now := time.Now()
		defer func() {
			s.bsnet.metrics.WantlistsSeconds.Observe(float64(time.Since(now)) / float64(time.Second))
		}()
	}

	return s.multiAttempt(ctx, func() error {
		return s.send(ctx, msg)
	})
}

// Perform a function with multiple attempts, and a timeout
func (s *streamMessageSender) multiAttempt(ctx context.Context, fn func() error) error {
	var err error
	var timer *time.Timer

	// Try to call the function repeatedly
	for i := 0; i < s.opts.MaxRetries; i++ {
		if err = fn(); err == nil {
			// Attempt was successful
			return nil
		}

		// Attempt failed

		// If the sender has been closed or the context cancelled, just bail out
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Protocol is not supported, so no need to try multiple times
		if errors.Is(err, multistream.ErrNotSupported[protocol.ID]{}) {
			s.bsnet.connectEvtMgr.MarkUnresponsive(s.to)
			return err
		}

		// Failed to send so reset stream and try again
		_ = s.Reset()

		// Failed too many times so mark the peer as unresponsive and return an error
		if i == s.opts.MaxRetries-1 {
			s.bsnet.connectEvtMgr.MarkUnresponsive(s.to)
			return err
		}

		if timer == nil {
			timer = time.NewTimer(s.opts.SendErrorBackoff)
			defer timer.Stop()
		} else {
			timer.Reset(s.opts.SendErrorBackoff)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			// wait a short time in case disconnect notifications are still propagating
			log.Infof("send message to %s failed but context was not Done: %s", s.to, err)
		}
	}
	return err
}

// Send a message to the peer
func (s *streamMessageSender) send(ctx context.Context, msg bsmsg.BitSwapMessage) error {
	start := time.Now()
	stream, err := s.Connect(ctx)
	if err != nil {
		log.Infof("failed to open stream to %s: %s", s.to, err)
		return err
	}

	// The send timeout includes the time required to connect
	// (although usually we will already have connected - we only need to
	// connect after a failed attempt to send)
	timeout := s.opts.SendTimeout - time.Since(start)
	if err = s.bsnet.msgToStream(ctx, stream, msg, timeout); err != nil {
		log.Infof("failed to send message to %s: %s", s.to, err)
		return err
	}

	return nil
}

func (bsnet *impl) Self() peer.ID {
	return bsnet.host.ID()
}

func (bsnet *impl) Ping(ctx context.Context, p peer.ID) ping.Result {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	res := <-ping.Ping(ctx, bsnet.host, p)
	return res
}

func (bsnet *impl) Latency(p peer.ID) time.Duration {
	return bsnet.host.Peerstore().LatencyEWMA(p)
}

func (bsnet *impl) Host() host.Host {
	return bsnet.host
}

// Indicates whether the given protocol supports HAVE / DONT_HAVE messages
func (bsnet *impl) SupportsHave(proto protocol.ID) bool {
	switch proto {
	case bsnet.protocolBitswapOneOne, bsnet.protocolBitswapOneZero, bsnet.protocolBitswapNoVers:
		return false
	}
	return true
}

func (bsnet *impl) msgToStream(ctx context.Context, s network.Stream, msg bsmsg.BitSwapMessage, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	if err := s.SetWriteDeadline(deadline); err != nil {
		log.Warnf("error setting deadline: %s", err)
	}

	bsnet.metrics.RequestsInFlight.Inc()
	defer bsnet.metrics.RequestsInFlight.Dec()

	// Older Bitswap versions use a slightly different wire format so we need
	// to convert the message to the appropriate format depending on the remote
	// peer's Bitswap version.
	switch s.Protocol() {
	case bsnet.protocolBitswapOneOne, bsnet.protocolBitswap:
		if err := msg.ToNetV1(s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	case bsnet.protocolBitswapOneZero, bsnet.protocolBitswapNoVers:
		if err := msg.ToNetV0(s); err != nil {
			log.Debugf("error: %s", err)
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol on remote: %s", s.Protocol())
	}

	atomic.AddUint64(&bsnet.stats.MessagesSent, 1)

	if err := s.SetWriteDeadline(time.Time{}); err != nil {
		log.Warnf("error resetting deadline: %s", err)
	}
	return nil
}

func (bsnet *impl) NewMessageSender(ctx context.Context, p peer.ID, opts *iface.MessageSenderOpts) (iface.MessageSender, error) {
	opts = setDefaultOpts(opts)

	sender := &streamMessageSender{
		to:    p,
		bsnet: bsnet,
		opts:  opts,
	}

	err := sender.multiAttempt(ctx, func() error {
		_, err := sender.Connect(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}

	return sender, nil
}

func setDefaultOpts(opts *iface.MessageSenderOpts) *iface.MessageSenderOpts {
	copy := *opts
	if opts.MaxRetries == 0 {
		copy.MaxRetries = 3
	}
	if opts.SendTimeout == 0 {
		copy.SendTimeout = maxSendTimeout
	}
	if opts.SendErrorBackoff == 0 {
		copy.SendErrorBackoff = 100 * time.Millisecond
	}
	return &copy
}

func sendTimeout(size int) time.Duration {
	timeout := sendLatency
	timeout += time.Duration((uint64(time.Second) * uint64(size)) / uint64(minSendRate))
	if timeout > maxSendTimeout {
		timeout = maxSendTimeout
	} else if timeout < minSendTimeout {
		timeout = minSendTimeout
	}
	return timeout
}

func (bsnet *impl) SendMessage(
	ctx context.Context,
	p peer.ID,
	outgoing bsmsg.BitSwapMessage,
) error {
	s, err := bsnet.newStreamToPeer(ctx, p)
	if err != nil {
		return err
	}

	timeout := sendTimeout(outgoing.Size())
	if err = bsnet.msgToStream(ctx, s, outgoing, timeout); err != nil {
		_ = s.Reset()
		return err
	}

	return s.Close()
}

func (bsnet *impl) newStreamToPeer(ctx context.Context, p peer.ID) (network.Stream, error) {
	return bsnet.host.NewStream(ctx, p, bsnet.supportedProtocols...)
}

func (bsnet *impl) Start(r ...iface.Receiver) {
	bsnet.receivers = r
	{
		connectionListeners := make([]iface.ConnectionListener, len(r))
		for i, v := range r {
			connectionListeners[i] = v
		}
		bsnet.connectEvtMgr = iface.NewConnectEventManager(connectionListeners...)
	}
	for _, proto := range bsnet.supportedProtocols {
		bsnet.host.SetStreamHandler(proto, bsnet.handleNewStream)
	}
	bsnet.host.Network().Notify((*netNotifiee)(bsnet))
	bsnet.connectEvtMgr.Start()
}

func (bsnet *impl) Stop() {
	bsnet.connectEvtMgr.Stop()
	bsnet.host.Network().StopNotify((*netNotifiee)(bsnet))
}

func (bsnet *impl) Connect(ctx context.Context, p peer.AddrInfo) error {
	if p.ID == bsnet.host.ID() {
		return nil
	}
	return bsnet.host.Connect(ctx, p)
}

func (bsnet *impl) DisconnectFrom(ctx context.Context, p peer.ID) error {
	return bsnet.host.Network().ClosePeer(p)
}

// handleNewStream receives a new stream from the network.
func (bsnet *impl) handleNewStream(s network.Stream) {
	defer s.Close()

	// In HTTPnet this metric measures sending the request and reading the
	// response.
	// In bitswap these are de-coupled, but we can measure them separately.
	bsnet.metrics.RequestsInFlight.Inc()
	defer bsnet.metrics.RequestsInFlight.Dec()

	if len(bsnet.receivers) == 0 {
		_ = s.Reset()
		return
	}

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	for {
		received, size, err := bsmsg.FromMsgReader(reader)
		if err != nil {
			if err != io.EOF {
				_ = s.Reset()
				for _, v := range bsnet.receivers {
					v.ReceiveError(err)
				}
				log.Debugf("bitswap net handleNewStream from %s error: %s", s.Conn().RemotePeer(), err)
			}
			return
		}

		bsnet.metrics.ResponseSizes.Observe(float64(size))
		p := s.Conn().RemotePeer()
		ctx := context.Background()
		log.Debugf("bitswap net handleNewStream from %s", s.Conn().RemotePeer())
		bsnet.connectEvtMgr.OnMessage(s.Conn().RemotePeer())
		atomic.AddUint64(&bsnet.stats.MessagesRecvd, 1)
		for _, v := range bsnet.receivers {
			v.ReceiveMessage(ctx, p, received)
		}
	}
}

func (bsnet *impl) TagPeer(p peer.ID, tag string, w int) {
	if bsnet.host == nil {
		return
	}
	bsnet.host.ConnManager().TagPeer(p, tag, w)
}

func (bsnet *impl) UntagPeer(p peer.ID, tag string) {
	if bsnet.host == nil {
		return
	}
	bsnet.host.ConnManager().UntagPeer(p, tag)
}

func (bsnet *impl) Protect(p peer.ID, tag string) {
	if bsnet.host == nil {
		return
	}
	bsnet.host.ConnManager().Protect(p, tag)
}

func (bsnet *impl) Unprotect(p peer.ID, tag string) bool {
	if bsnet.host == nil {
		return false
	}
	return bsnet.host.ConnManager().Unprotect(p, tag)
}

func (bsnet *impl) Stats() iface.Stats {
	return iface.Stats{
		MessagesRecvd: atomic.LoadUint64(&bsnet.stats.MessagesRecvd),
		MessagesSent:  atomic.LoadUint64(&bsnet.stats.MessagesSent),
	}
}

type netNotifiee impl

func (nn *netNotifiee) impl() *impl {
	return (*impl)(nn)
}

func (nn *netNotifiee) Connected(n network.Network, v network.Conn) {
	// ignore transient connections
	if v.Stat().Limited {
		return
	}

	nn.impl().connectEvtMgr.Connected(v.RemotePeer())
}

func (nn *netNotifiee) Disconnected(n network.Network, v network.Conn) {
	// Only record a "disconnect" when we actually disconnect.
	if n.Connectedness(v.RemotePeer()) == network.Connected {
		return
	}

	nn.impl().connectEvtMgr.Disconnected(v.RemotePeer())
}
func (nn *netNotifiee) OpenedStream(n network.Network, s network.Stream) {}
func (nn *netNotifiee) ClosedStream(n network.Network, v network.Stream) {}
func (nn *netNotifiee) Listen(n network.Network, a ma.Multiaddr)         {}
func (nn *netNotifiee) ListenClose(n network.Network, a ma.Multiaddr)    {}
