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

// Blacklist
type Blacklist struct {
	// Options
	Options options.BlacklistOptions

	// Blacklisted peers by ID
	byPeerID map[peer.ID]BlacklistEntry

	// Mutex for accessing from multiple threads
	blacklistMutex sync.Mutex
}

func (bl *Blacklist) AddPeerToBlacklist(perr PeerError) {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	bl.byPeerID[perr.PeerID] = BlacklistEntry{
		PeerID:         perr.PeerID,
		Error:          perr.Error,
		ExpirationTime: time.Now().Add(time.Duration(bl.Options.BlacklistMs) * time.Millisecond),
	}
}

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

func (bl *Blacklist) GetBlacklistEntry(peerID peer.ID) (BlacklistEntry, bool) {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	entry, hasEntry := bl.byPeerID[peerID]
	return entry, hasEntry
}

func (bl *Blacklist) IsPeerBlacklisted(peerID peer.ID) bool {
	bl.blacklistMutex.Lock()
	defer bl.blacklistMutex.Unlock()
	_, hasEntry := bl.byPeerID[peerID]
	return hasEntry
}

func NewBlacklist(opts options.BlacklistOptions) *Blacklist {
	bl := Blacklist{
		Options:  opts,
		byPeerID: make(map[peer.ID]BlacklistEntry),
	}
	return &bl
}
