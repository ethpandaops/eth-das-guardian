package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBeaconStateUnmarshal guards against a regression where the `slot` field
// carried a `,string` JSON tag. phase0.Slot already implements json.Unmarshaler
// expecting a quoted value, so `,string` made the decoder strip the quotes
// first and the scan failed on every head state ("input malformed" for slots
// below 100, "invalid prefix" at or above 100).
func TestBeaconStateUnmarshal(t *testing.T) {
	for _, slot := range []string{"5", "42", "101", "1000"} {
		body := `{"version":"gloas","data":{"slot":"` + slot + `",` +
			`"latest_block_header":{"slot":"` + slot + `","proposer_index":"1",` +
			`"parent_root":"0x1843daa02701a9df7bc3ce03410d894e16597a4a4e7ffd8a18512a3f73685288",` +
			`"state_root":"0x0000000000000000000000000000000000000000000000000000000000000000",` +
			`"body_root":"0xd08a58fd3406573e25ddda0ffc318f5f614ad8d04b264b3eb4c383c921675b11"},` +
			`"finalized_checkpoint":{"epoch":"0",` +
			`"root":"0x0000000000000000000000000000000000000000000000000000000000000000"},` +
			`"validators":[]}}`

		var state BeaconState
		require.NoErrorf(t, json.Unmarshal([]byte(body), &state), "slot %s", slot)
		require.Equal(t, "gloas", state.Version)
		require.EqualValues(t, mustParseUint(t, slot), state.Data.Slot)
	}
}

func mustParseUint(t *testing.T, s string) uint64 {
	t.Helper()
	var v uint64
	for _, c := range s {
		require.True(t, c >= '0' && c <= '9')
		v = v*10 + uint64(c-'0')
	}
	return v
}
