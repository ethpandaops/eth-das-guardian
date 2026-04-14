package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethpandaops/go-eth2-client/spec"
	"github.com/ethpandaops/go-eth2-client/spec/electra"
	"github.com/ethpandaops/go-eth2-client/spec/gloas"
	"github.com/pkg/errors"
)

var (
	BlockBase        = "eth/v2/beacon/blocks/"
	ErrBlockNotFound = fmt.Errorf("block not found")
)

type beaconBlockEnvelope struct {
	Version             string          `json:"version"`
	ExecutionOptimistic bool            `json:"execution_optimistic"`
	Finalized           bool            `json:"finalized"`
	Data                json.RawMessage `json:"data"`
}

func (c *Client) GetBeaconBlock(ctx context.Context, slot any) (*spec.VersionedSignedBeaconBlock, error) {
	blockQuery := BlockBase
	switch s := slot.(type) {
	case int, int32, int64, uint, uint32, uint64:
		blockQuery = blockQuery + fmt.Sprintf("%d", s)
	case string:
		blockQuery = blockQuery + s
	default:
		return nil, fmt.Errorf("unrecognized slot %s", slot)
	}

	resp, err := c.get(ctx, c.cfg.QueryTimeout, blockQuery, "")
	if err != nil {
		if strings.Contains(err.Error(), "404 Not Found") {
			return new(spec.VersionedSignedBeaconBlock), ErrBlockNotFound
		}
		return nil, errors.Wrap(err, "requesting beacon-block")
	}

	envelope := &beaconBlockEnvelope{}
	if err := json.Unmarshal(resp, envelope); err != nil {
		return nil, errors.Wrap(err, "unmarshaling beacon-block envelope")
	}

	versionedBlock := &spec.VersionedSignedBeaconBlock{}
	switch strings.ToLower(envelope.Version) {
	case "electra":
		block := &electra.SignedBeaconBlock{}
		if err := json.Unmarshal(envelope.Data, block); err != nil {
			return nil, errors.Wrap(err, "unmarshaling electra signed beacon block")
		}
		versionedBlock.Version = spec.DataVersionElectra
		versionedBlock.Electra = block
	case "fulu":
		block := &electra.SignedBeaconBlock{}
		if err := json.Unmarshal(envelope.Data, block); err != nil {
			return nil, errors.Wrap(err, "unmarshaling fulu signed beacon block")
		}
		versionedBlock.Version = spec.DataVersionFulu
		versionedBlock.Fulu = block
	case "gloas":
		block := &gloas.SignedBeaconBlock{}
		if err := json.Unmarshal(envelope.Data, block); err != nil {
			return nil, errors.Wrap(err, "unmarshaling gloas signed beacon block")
		}
		versionedBlock.Version = spec.DataVersionGloas
		versionedBlock.Gloas = block
	default:
		return nil, fmt.Errorf("unsupported beacon block version %q", envelope.Version)
	}

	return versionedBlock, nil
}
