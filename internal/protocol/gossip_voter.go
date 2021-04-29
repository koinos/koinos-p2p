package protocol

import (
	"github.com/libp2p/go-libp2p-core/peer"
)

// GossipVoter keeps a tally of whether each peer is contemporary or not.
type GossipVoter struct {
	ByPeer          map[peer.ID]bool
	YesCount        int
	NoCount         int
	EnableGossip    bool
	GossipDisableBp int
	GossipEnableBp  int
	AlwaysDisable   bool
	AlwaysEnable    bool
}

// NewGossipVoter creates a GossipVoter object
func NewGossipVoter(d int, e int, alwaysDisable bool, alwaysEnable bool) *GossipVoter {
	v := GossipVoter{
		ByPeer:          make(map[peer.ID]bool),
		YesCount:        0,
		NoCount:         0,
		EnableGossip:    false,
		GossipDisableBp: d,
		GossipEnableBp:  e,
		AlwaysDisable:   alwaysDisable,
		AlwaysEnable:    alwaysEnable,
	}
	return &v
}

// Vote processes a PeerIsContemporary message
func (gv *GossipVoter) Vote(v PeerIsContemporary) {
	oldVote, hasOldVote := gv.ByPeer[v.PeerID]
	if hasOldVote {
		if oldVote == v.IsContemporary {
			return
		}
		if v.IsContemporary {
			gv.NoCount--
			gv.YesCount++
		} else {
			gv.NoCount++
			gv.YesCount--
		}
		gv.ByPeer[v.PeerID] = v.IsContemporary
	} else {
		if v.IsContemporary {
			gv.YesCount++
		} else {
			gv.NoCount++
		}
	}
	gv.EnableGossip = gv.computeEnableGossip()
}

func (gv *GossipVoter) computeEnableGossip() bool {
	if gv.AlwaysDisable {
		return false
	}
	if gv.AlwaysEnable {
		return true
	}
	n := gv.YesCount + gv.NoCount

	// All no's always returns false.  This also handles n == 0 case.
	if n == gv.NoCount {
		return false
	}

	// All yes's always returns true.
	if n == gv.YesCount {
		return true
	}

	// Compute thresholds as number of peers
	enableThreshold := (n * gv.GossipEnableBp) / 10000
	disableThreshold := (n * gv.GossipDisableBp) / 10000

	// Clamp to enforce 0 <= disableThreshold < enableThreshold
	if enableThreshold < 1 {
		enableThreshold = 1
	}
	if disableThreshold > enableThreshold-1 {
		disableThreshold = enableThreshold - 1
	}

	// Force enable / disable if threshold is met
	doEnable := (gv.YesCount >= enableThreshold)
	doDisable := (gv.YesCount <= disableThreshold)

	if doEnable {
		return true
	}

	if doDisable {
		return false
	}

	// Hysteresis if in between enable / disable thresholds
	return gv.EnableGossip
}
