package api

import (
	"context"
	"encoding/json"

	"github.com/ethpandaops/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

var BeaconStateBase = "eth/v2/debug/beacon/states/head"

// BeaconStateData holds only the fields of the head beacon state that
// eth-das-guardian actually consumes. A minimal struct keeps decoding
// version-agnostic across electra/fulu/gloas — JSON unmarshal silently
// ignores unknown fields, so newer forks' additions don't break parsing.
type BeaconStateData struct {
	Slot                phase0.Slot               `json:"slot,string"`
	LatestBlockHeader   *phase0.BeaconBlockHeader `json:"latest_block_header"`
	FinalizedCheckpoint *phase0.Checkpoint        `json:"finalized_checkpoint"`
	Validators          []json.RawMessage         `json:"validators"`
}

type BeaconState struct {
	Version             string          `json:"version"`
	ExecutionOptimistic bool            `json:"execution_optimistic"`
	Finalized           bool            `json:"finalized"`
	Data                BeaconStateData `json:"data"`
}

func (c *Client) GetBeaconStateHead(ctx context.Context) (BeaconState, error) {
	var state BeaconState

	resp, err := c.get(ctx, c.cfg.QueryTimeout, BeaconStateBase, "")
	if err != nil {
		return state, errors.Wrap(err, "requesting beacon-state")
	}

	if err := json.Unmarshal(resp, &state); err != nil {
		return state, errors.Wrap(err, "unmarshaling beacon-state from http request")
	}

	return state, nil
}
