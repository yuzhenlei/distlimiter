package distlimiter

import (
	"log"
	"os/exec"
	"sort"
	"sync"
	"time"
)

type Peer struct {
	mu                sync.RWMutex
	id                string
	qps               uint32
	totalQPS          uint32
	remote            RemoteStore
	peerIDs           []string
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	heartbeatFail     bool
	clearInterval     time.Duration
	lastClearTime     time.Time
	onSendDone        func(error)
	onPullDone        func(error)
	onClear           func(until time.Time) error
}

type peerOptions struct {
	Id                      string
	HeartbeatSeconds        uint32
	HeartbeatTimeoutSeconds uint32
	ClearInterval           time.Duration
	OnSendDone              func(error)
	OnPullDone              func(error)
	OnClear                 func(until time.Time) error
}

func NewPeer(totalQPS uint32, remote RemoteStore, options *peerOptions) *Peer {
	id := options.Id
	if id == "" {
		id = GenUUID()
	}
	hbseconds := options.HeartbeatSeconds
	if hbseconds < 1 {
		hbseconds = defaultHeartbeatSeconds
	}
	hbtimeout := options.HeartbeatTimeoutSeconds
	if hbtimeout == 0 {
		hbtimeout = defaultHeartbeatTimeoutSeconds
	}
	if hbtimeout < hbseconds {
		panic("heartbeat timeout must not less than interval")
	}
	clearInterval := options.ClearInterval
	if clearInterval <= 0 {
		clearInterval = 100 * time.Duration(hbseconds) * time.Second
	}
	peer := &Peer{
		id:                id,
		qps:               0,
		totalQPS:          totalQPS,
		remote:            remote,
		heartbeatInterval: time.Duration(hbseconds) * time.Second,
		heartbeatTimeout:  time.Duration(hbtimeout) * time.Second,
		clearInterval:     clearInterval,
		onSendDone:        options.OnSendDone,
		onPullDone:        options.OnPullDone,
		onClear:           options.OnClear,
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
	sort.Strings(peerIDs)
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peerIDs) == 0 {
		peer.qps = 0
		return
	}
	peer.peerIDs = peerIDs
	isFoundMe := false
	for _, peerId := range peerIDs {
		if peerId == peer.GetId() {
			isFoundMe = true
			break
		}
	}
	if isFoundMe {
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
	log.Printf("peer[%s] curr qps: %d\n", peer.GetId(), peer.qps)
}

func (peer *Peer) Send() {
	err := peer.remote.Send(time.Now(), peer.GetId())
	if err != nil {
		peer.heartbeatFail = true
		log.Printf("peer[%s] send fail: %s", peer.GetId(), err.Error())
	} else {
		peer.heartbeatFail = false
	}
	if peer.onSendDone != nil {
		peer.onSendDone(err)
	}
}

func (peer *Peer) Pull() {
	if peer.heartbeatFail {
		return
	}
	min := time.Now().Add(-peer.heartbeatTimeout)
	// max不取now的原因是，假如peerA.pull.time在peerB.send.time之前，但是peerB.send.time却提前到达了remote，造成pull不到的情况
	max := time.Now().Add(peer.heartbeatTimeout)
	ids, err := peer.remote.Pull(min, max)
	if err != nil {
		log.Printf("peer[%s] pull fail: %s", peer.GetId(), err.Error())
	}
	peer.AdjustQPS(ids)
	if peer.onPullDone != nil {
		peer.onPullDone(err)
	}
}

func (peer *Peer) clear() {
	peer.mu.Lock()
	isSelected := false
	lastClearTime := peer.lastClearTime
	if len(peer.peerIDs) > 0 && peer.peerIDs[0] == peer.GetId() &&
		peer.lastClearTime.Add(peer.clearInterval).Before(time.Now()) {
		isSelected = true
		peer.lastClearTime = time.Now()
	}
	peer.mu.Unlock()
	if isSelected && peer.onClear != nil {
		log.Printf("call onClear(until: %s)", lastClearTime.Format(time.Stamp))
		if err := peer.onClear(lastClearTime); err != nil {
			log.Printf("call onClear(until: %s) fail: %s", lastClearTime.Format(time.Stamp), err.Error())
		}
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
				peer.clear()
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
