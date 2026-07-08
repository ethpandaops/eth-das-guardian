package dasguardian

import (
	"fmt"
)

var (
	GossipBeaconBlock         = "/eth/%x/beacon_block/ssz_snappy"
	GossipProposerPreferences = "/eth2/%x/proposer_preferences/ssz_snappy"
)

func getMandatoryTopics(forkDigest []byte) []string {
	return []string{
		fmt.Sprintf(GossipBeaconBlock, forkDigest),
	}
}
