package distlimiter

import (
	"os/exec"
)

type Peer struct {
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
	return peer.qps
}

func (peer *Peer) AdjustQPS(peerIDs []string) {
	// TODO 根据peerIDs大小决定余数qps的分配
	peer.qps = peer.totalQPS / uint32(len(peerIDs))
}

func (peer *Peer) Send(callback func(error)) {
	if err := peer.remote.Send(peer.GetId()); err != nil {
		callback(err)
		return
	}
	callback(nil)
}

func (peer *Peer) Pull(callback func(error)) {
	ids, err := peer.remote.Pull()
	if err != nil {
		callback(err)
		return
	}
	peer.AdjustQPS(ids)
	callback(nil)
}