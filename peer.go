package distlimiter

import (
	"os/exec"
	"sort"
	"sync"
)

type Peer struct {
	mu sync.RWMutex
	id       string
	qps      uint32
	totalQPS uint32
	remote RemoteStore
}

func NewPeer(totalQPS uint32, remote RemoteStore) *Peer {
	uuid, err := exec.Command("/usr/bin/uuidgen").Output()
	if err != nil {
		panic(err.Error())
	}
	return &Peer{
		id: string(uuid),
		qps: 0,
		totalQPS: totalQPS,
		remote: remote,
	}
}

func (peer *Peer) GetId() string {
	return peer.id
}

func (peer *Peer) GetQPS() uint32 {
	peer.mu.RLock()
	defer peer.mu.RUnlock()
	return peer.qps
}

func (peer *Peer) AdjustQPS(peerIDs []string) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	peerCount := uint32(len(peerIDs))
	peer.qps = peer.totalQPS / peerCount
	sort.Strings(peerIDs)
	mod := int(peer.totalQPS % peerCount)
	for i := 0; i < mod; i++ {
		if peerIDs[i] == peer.GetId() {
			peer.qps++
		}
	}
}

func (peer *Peer) Send(callback func(error)) {
	err := peer.remote.Send(peer.GetId())
	callback(err)
}

func (peer *Peer) Pull(callback func(error)) {
	ids, err := peer.remote.Pull()
	if err == nil {
		peer.AdjustQPS(ids)
	}
	callback(err)
}