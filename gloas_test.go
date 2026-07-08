package dasguardian

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ethpandaops/go-eth2-client/spec/bellatrix"
	"github.com/ethpandaops/go-eth2-client/spec/gloas"
	"github.com/ethpandaops/go-eth2-client/spec/phase0"
	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProposerPreferencesObserver_EndToEnd spins up two libp2p hosts in process,
// has one Subscribe via the new proposer_preferences gossip path and the other
// Publish a SignedProposerPreferences. It then asserts the observer captures
// the message, decodes it correctly, and attributes it to the sender peer.
//
// If this test passes but a real-network scan returns count:0, the empty result
// means no validator on the scanned peer was due to propose during the wait
// window — not a bug in the wiring.
func TestProposerPreferencesObserver_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	subscriber, subPubsub := newPubsubHost(t, ctx)
	defer subscriber.Close()
	publisher, pubPubsub := newPubsubHost(t, ctx)
	defer publisher.Close()

	connectHosts(t, ctx, subscriber, publisher)

	topicName := "/eth2/deadbeef/proposer_preferences/ssz_snappy"

	observer, err := joinProposerPreferences(ctx, log.New(), subPubsub, topicName)
	require.NoError(t, err)
	defer observer.close()

	pubTopic, err := pubPubsub.Join(topicName)
	require.NoError(t, err)
	defer pubTopic.Close()

	// Give gossipsub a couple of heartbeats to graft the mesh between the two
	// hosts. Without this the publish lands before mesh peers are selected and
	// the message is dropped.
	require.Eventually(t, func() bool {
		return len(pubTopic.ListPeers()) >= 1
	}, 5*time.Second, 100*time.Millisecond, "publisher never saw subscriber as a topic peer")

	want := &gloas.SignedProposerPreferences{
		Message: &gloas.ProposerPreferences{
			DependentRoot:  phase0.Root{0xaa, 0xbb, 0xcc, 0xdd},
			ProposalSlot:   phase0.Slot(123_456),
			ValidatorIndex: phase0.ValidatorIndex(42),
			FeeRecipient:   bellatrix.ExecutionAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14},
			TargetGasLimit: 36_000_000,
		},
		Signature: phase0.BLSSignature{},
	}

	sszData, err := want.MarshalSSZ()
	require.NoError(t, err)
	wire := snappy.Encode(nil, sszData)

	require.NoError(t, pubTopic.Publish(ctx, wire))

	require.Eventually(t, func() bool {
		observer.mu.RLock()
		defer observer.mu.RUnlock()
		return len(observer.observations) >= 1
	}, 10*time.Second, 100*time.Millisecond, "observer never received the published SignedProposerPreferences")

	observer.mu.RLock()
	defer observer.mu.RUnlock()

	require.Len(t, observer.observations, 1)
	got := observer.observations[0]

	require.NotNil(t, got.Message)
	require.NotNil(t, got.Message.Message)
	assert.Equal(t, want.Message.DependentRoot, got.Message.Message.DependentRoot)
	assert.Equal(t, want.Message.ProposalSlot, got.Message.Message.ProposalSlot)
	assert.Equal(t, want.Message.ValidatorIndex, got.Message.Message.ValidatorIndex)
	assert.Equal(t, want.Message.FeeRecipient, got.Message.Message.FeeRecipient)
	assert.Equal(t, want.Message.TargetGasLimit, got.Message.Message.TargetGasLimit)
	assert.Equal(t, publisher.ID(), got.From)

	// observationsFrom should bucket the message under the publisher peer ID.
	fromPub := observer.observationsFrom(publisher.ID())
	assert.Len(t, fromPub, 1)
	assert.Empty(t, observer.observationsFrom(peer.ID("not-a-real-peer")))
}

func newPubsubHost(t *testing.T, ctx context.Context) (host.Host, *pubsub.PubSub) {
	t.Helper()
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Ping(false),
	)
	require.NoError(t, err)

	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
	)
	require.NoError(t, err)
	return h, ps
}

func connectHosts(t *testing.T, ctx context.Context, a, b host.Host) {
	t.Helper()
	a.Peerstore().AddAddrs(b.ID(), b.Addrs(), peerstore.PermanentAddrTTL)
	b.Peerstore().AddAddrs(a.ID(), a.Addrs(), peerstore.PermanentAddrTTL)
	require.NoError(t, a.Connect(ctx, peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}))
	require.Eventually(t, func() bool {
		return len(a.Network().Peers()) >= 1 && len(b.Network().Peers()) >= 1
	}, 5*time.Second, 50*time.Millisecond, fmt.Sprintf("hosts %s and %s never observed each other", a.ID(), b.ID()))
}
