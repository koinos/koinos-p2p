package protocol

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/koinos/koinos-p2p/internal/options"
)

// PeerError represents an error in communication with a peer
type PeerError struct {
	PeerID peer.ID
	Error  error
}

// BlacklistEntry represents an error in communication with a peer
type BlacklistEntry struct {
	PeerID         peer.ID
	Error          error
	ExpirationTime time.Time
}

// Blacklist stores a list of peers that have recently been disconnected due to errors.
//
// The purpose of this Blacklist is mainly to prevent spamming reconnection attempts
// to a peer that's misconfigured or running the wrong version over short timescales
// (a minute or so).  It's not intended to be a long-lived list of known bad actors.
//
// The Blacklist also saves the reason for disconnecting each peer.  The saved reasons
// are mainly useful for checking that a particular error happened in unit tests.
//
type Blacklist struct {
	// Options
	Options options.BlacklistOptions

	// Blacklisted peers by ID
	byPeerID map[peer.ID]BlacklistEntry

	// Mutex for accessing from multiple threads
	blacklistMutex sync.Mutex
}

// AddPeerToBlacklist adds the given PeerError to the blacklist.
func (bl *Blacklist) AddPeerToBlacklist(perr PeerError) {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	bl.byPeerID[perr.PeerID] = BlacklistEntry{
		PeerID:         perr.PeerID,
		Error:          perr.Error,
		ExpirationTime: time.Now().Add(time.Duration(bl.Options.BlacklistMs) * time.Millisecond),
	}
}

// RemoveExpiredBlacklistEntries walks the blacklist and removes expired peers.
//
// TODO:  Instead of scanning all entries, use a more efficient data structure (e.g. min-heap)
func (bl *Blacklist) RemoveExpiredBlacklistEntries() {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	now := time.Now()
	keysToDelete := make([]peer.ID, 0)
	for k, e := range bl.byPeerID {
		if e.ExpirationTime.Before(now) {
			keysToDelete = append(keysToDelete, k)
		}
	}
	for _, k := range keysToDelete {
		delete(bl.byPeerID, k)
	}
}

// GetBlacklistEntry returns the entry for the given peer ID, and a flag indicating whether the entry is valid.
func (bl *Blacklist) GetBlacklistEntry(peerID peer.ID) (BlacklistEntry, bool) {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	entry, hasEntry := bl.byPeerID[peerID]
	return entry, hasEntry
}

// IsPeerBlacklisted returns true if the peer is currently on the blacklist.
func (bl *Blacklist) IsPeerBlacklisted(peerID peer.ID) bool {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	_, hasEntry := bl.byPeerID[peerID]
	return hasEntry
}

// NewBlacklist creates a new blacklist.
func NewBlacklist(opts options.BlacklistOptions) *Blacklist {
	bl := Blacklist{
		Options:  opts,
		byPeerID: make(map[peer.ID]BlacklistEntry),
	}
	return &bl
}
