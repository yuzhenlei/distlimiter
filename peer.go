package distlimiter

import (
	"log"
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
	onSendDone func(error)
	onPullDone func(error)
}

type peerOptions struct {
	Id string
	HeartbeatSeconds uint32
	OnSendDone func(error)
	OnPullDone func(error)
}

func NewPeer(totalQPS uint32, remote RemoteStore, options *peerOptions) *Peer {
	id := options.Id
	if id == "" {
		id = GenUUID()
	}
	seconds := options.HeartbeatSeconds
	if seconds < 1 {
		seconds = defaultHeartbeatSeconds
	}
	peer := &Peer{
		id: id,
		qps: 0,
		totalQPS: totalQPS,
		remote: remote,
		heartbeatInterval: time.Duration(seconds) * time.Second,
		onSendDone: options.OnSendDone,
		onPullDone: options.OnPullDone,
	}
	peer.heartbeat()
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
	if len(peerIDs) == 0 {
		return
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	isFoundMe := false
	for _, peerId := range peerIDs {
		if peerId == peer.GetId() {
			isFoundMe = true
			break
		}
	}
	if isFoundMe {
		sort.Strings(peerIDs)
		peerCount := uint32(len(peerIDs))
		peer.qps = peer.totalQPS / peerCount
		mod := int(peer.totalQPS % peerCount)
		for i := 0; i < mod; i++ {
			if peerIDs[i] == peer.GetId() {
				peer.qps++
			}
		}
	} else {
		peer.qps = 0
	}
	log.Printf("id[%s] curr qps: %d\n", peer.id, peer.qps)
}

// FIXME 需要清理报备历史
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

func (peer *Peer) heartbeat() {
	go func() {
		tick := time.Tick(peer.heartbeatInterval)
		for {
			select {
			case <-tick:
				peer.Pull()
				peer.Send()
			}
		}
	}()
}

func GenUUID() string {
	uuid, err := exec.Command("/usr/bin/uuidgen").Output()
	if err != nil {
		panic(err.Error())
	}
	return string(uuid)
}