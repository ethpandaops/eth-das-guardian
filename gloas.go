package dasguardian

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethpandaops/go-eth2-client/spec/gloas"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	log "github.com/sirupsen/logrus"
)

// ObservedProposerPreference records a SignedProposerPreferences gossip message
// sniffed off the wire, together with the peer that delivered it to us.
type ObservedProposerPreference struct {
	Message    *gloas.SignedProposerPreferences
	From       peer.ID
	ReceivedAt time.Time
}

// proposerPreferencesObserver subscribes to the Gloas `proposer_preferences`
// gossip topic and keeps an in-memory view of the SignedProposerPreferences
// messages it has observed.
type proposerPreferencesObserver struct {
	logger log.FieldLogger
	sub    *pubsub.Subscription

	mu           sync.RWMutex
	observations []*ObservedProposerPreference
}

// joinProposerPreferences joins, subscribes to and starts reading the Gloas
// `proposer_preferences` gossip topic. The returned observer is safe for
// concurrent use; the reader goroutine exits when ctx is cancelled or the
// subscription is closed.
func joinProposerPreferences(
	ctx context.Context,
	logger log.FieldLogger,
	ps *pubsub.PubSub,
	topicName string,
) (*proposerPreferencesObserver, error) {
	topic, err := ps.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("join pubsub topic %s: %w", topicName, err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("subscribe to pubsub topic %s: %w", topicName, err)
	}

	obs := &proposerPreferencesObserver{
		logger:       logger.WithField("topic", topicName),
		sub:          sub,
		observations: make([]*ObservedProposerPreference, 0, 16),
	}

	go obs.readLoop(ctx)

	return obs, nil
}

func (o *proposerPreferencesObserver) readLoop(ctx context.Context) {
	for {
		msg, err := o.sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			o.logger.WithError(err).Debug("proposer_preferences subscription read failed")
			return
		}

		decoded, err := snappy.Decode(nil, msg.Data)
		if err != nil {
			o.logger.WithError(err).Debug("failed to snappy-decode proposer_preferences message")
			continue
		}

		signed := &gloas.SignedProposerPreferences{}
		if err := signed.UnmarshalSSZ(decoded); err != nil {
			o.logger.WithError(err).Debug("failed to SSZ-decode SignedProposerPreferences")
			continue
		}

		o.mu.Lock()
		o.observations = append(o.observations, &ObservedProposerPreference{
			Message:    signed,
			From:       msg.ReceivedFrom,
			ReceivedAt: time.Now(),
		})
		o.mu.Unlock()

		if signed.Message != nil {
			o.logger.WithFields(log.Fields{
				"validator_index": uint64(signed.Message.ValidatorIndex),
				"proposal_slot":   uint64(signed.Message.ProposalSlot),
				"fee_recipient":   fmt.Sprintf("0x%x", signed.Message.FeeRecipient),
				"gas_limit":       signed.Message.GasLimit,
				"received_from":   msg.ReceivedFrom.String(),
			}).Info("observed SignedProposerPreferences")
		}
	}
}

// observationsFrom returns the SignedProposerPreferences observations that were
// delivered to us by the given peer. Returns an empty slice if no observations
// for that peer exist (yet).
func (o *proposerPreferencesObserver) observationsFrom(pid peer.ID) []*ObservedProposerPreference {
	o.mu.RLock()
	defer o.mu.RUnlock()

	out := make([]*ObservedProposerPreference, 0)
	for _, obs := range o.observations {
		if obs.From == pid {
			out = append(out, obs)
		}
	}
	return out
}

// close terminates the underlying subscription. The reader goroutine will exit
// on its next read.
func (o *proposerPreferencesObserver) close() {
	if o.sub != nil {
		o.sub.Cancel()
	}
}

// visualizeProposerPreferences renders a slice of observations into a
// log-friendly map, matching the conventions used by the other visualize* helpers.
func visualizeProposerPreferences(observations []*ObservedProposerPreference) map[string]any {
	out := make(map[string]any, 1)
	out["count"] = len(observations)
	if len(observations) == 0 {
		return out
	}

	entries := make([]map[string]any, 0, len(observations))
	for _, obs := range observations {
		if obs == nil || obs.Message == nil || obs.Message.Message == nil {
			continue
		}
		prefs := obs.Message.Message
		entries = append(entries, map[string]any{
			"validator_index": uint64(prefs.ValidatorIndex),
			"proposal_slot":   uint64(prefs.ProposalSlot),
			"fee_recipient":   fmt.Sprintf("0x%x", prefs.FeeRecipient),
			"gas_limit":       prefs.GasLimit,
			"dependent_root":  fmt.Sprintf("0x%x", prefs.DependentRoot),
			"received_at":     obs.ReceivedAt.Format(time.RFC3339Nano),
		})
	}
	out["entries"] = entries
	return out
}
