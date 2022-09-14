package distlimiter

import (
	"os/exec"
	"sort"
	"sync"
	"time"
)

type Peer struct {
	mu sync.RWMutex
	id       string
	qps      uint32
	totalQPS uint32
	remote RemoteStore
	heartbeatInterval time.Duration
	heartbeat *Heartbeat
	onSendDone func(error)
	onPullDone func(error)
}

func NewPeer(totalQPS uint32, remote RemoteStore) *Peer {
	uuid, err := exec.Command("/usr/bin/uuidgen").Output()
	if err != nil {
		panic(err.Error())
	}
	peer := &Peer{
		id: string(uuid),
		qps: 0,
		totalQPS: totalQPS,
		remote: remote,
		heartbeatInterval: time.Duration(defaultHeartbeatInterval) * time.Second,
	}
	peer.heartbeat = NewHeartbeat(peer.heartbeatInterval, peer)
	peer.heartbeat.Go()
	return peer
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

func (peer *Peer) Send() {
	err := peer.remote.Send(time.Now(), peer.GetId())
	if peer.onSendDone != nil {
		peer.onSendDone(err)
	}
}

func (peer *Peer) Pull() {
	min := time.Now().Add(-peer.heartbeatInterval)
	max := time.Now()
	ids, err := peer.remote.Pull(min, max)
	if err == nil {
		peer.AdjustQPS(ids)
	}
	if peer.onPullDone != nil {
		peer.onPullDone(err)
	}
}