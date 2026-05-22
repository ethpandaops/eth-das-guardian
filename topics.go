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

// getGloasTopics returns the additional gossip topics that are only valid once
// the Gloas (EIP-7732) fork is active.
func getGloasTopics(forkDigest []byte) []string {
	return []string{
		fmt.Sprintf(GossipProposerPreferences, forkDigest),
	}
}
