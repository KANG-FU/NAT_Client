package impl

import (
	"math/rand"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// This function will be called when a ChatMessage is received.
func (n *node) handleChatMessage(msg types.Message, pkt transport.Packet) error {
	msg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	return nil
}

/* 
This function will be called when a RumorsMessage is received.
Main functionality includes 
1. check if the rumor is expected. 
If it is, you need to update the routing table following rules. 
Also, you need to process the packet.
2. if there is any new rumor, you need to send to a new neighbour
3. send ack back to rumor src
*/
func (n *node) handleRumorsMessage(msg types.Message, pkt transport.Packet) error {
	n.rumorLock.Lock()
	defer n.rumorLock.Unlock()
	rumors, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// flag to represent if there is any new rumor
	flagNewRumor := false
	// map to determine if the routing table is updated or not
	flagUpdatedRT := make(map[string]bool)

	// Process Rumors
	for _, rumor := range rumors.Rumors {
		if rumor.Sequence != n.status[rumor.Origin]+1 {
			// rumor is not expected, continue the loop
			continue
		}
		// rumor is expected and set the flag to true
		flagNewRumor = true

		// update the routing entry of a peer each time we process a new rumor from
		// that peer and we are not already directly connected to that peer 
		// The routing’s entry for a peer should be the relay address 
		if rumor.Origin != n.conf.Socket.GetAddress() && !flagUpdatedRT[rumor.Origin] && !n.neighbors.has(rumor.Origin) {
			n.SetRoutingEntry(rumor.Origin, pkt.Header.RelayedBy)
			flagUpdatedRT[rumor.Origin] = true
		}

		newPkt := transport.Packet{
			Header: pkt.Header,
			Msg:    rumor.Msg,
		}

		// process the packet
		if err := n.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
			log.Err(err).Msg("failed to process packet")
		}

		// update the node status
		n.status[rumor.Origin]++
		n.rumorsLog[rumor.Origin] = append(n.rumorsLog[rumor.Origin], rumor)
	}

	// Send the RumorMessage to another random neighbor in the case where one of the Rumor is new
	if flagNewRumor {
		if !(n.neighbors.len() == 0 || n.neighbors.hasOnlyNeighbour(pkt.Header.Source)) {
			neighbor := n.neighbors.getNewRandom(pkt.Header.Source)
			header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
			pkt.Header = &header
			if err := n.conf.Socket.Send(neighbor, pkt, sendTimeout); err != nil {
				log.Err(err).Msg("failed to send rumor to random neibough")
			}	
		}
	}

	// Send back an AckMessage to the source
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	ackMsg := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        n.status,
	}
	ackPkt := transport.Packet{
		Header: &header,
		Msg:    n.getMarshalledMsg(&ackMsg),
	}

	if err := n.conf.Socket.Send(pkt.Header.Source, ackPkt, sendTimeout); err != nil {
		log.Err(err).Msg("failed to send rumor ack")
		return err
	}

	return nil
}

/*
This function will be called when a AckMessage is received.
Main functionality includes 
1. The peer stops waiting (stops the timer) for the ack corresponding to that PacketID.
2. Process the status message contained in the AckMessage (use the registry.ProcessPacket function).
*/
func (n *node) handleAckMessage(msg types.Message, pkt transport.Packet) error {
	ack,ok:= msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// check it the packetID is in the sync Map or not
	value, ok := n.ack.Load(ack.AckedPacketID)
	if ok {
		ackChan, ok:= value.(chan int)
		if !ok {
			return xerrors.Errorf("channel error")
		}
		select {
			case ackChan <- 1:
				close(ackChan)
				n.ack.Delete(ack.AckedPacketID)
			default:
		}
	}
	
	newPkt := transport.Packet{
		Header: pkt.Header,
		Msg:    n.getMarshalledMsg(&ack.Status),
	}
	
	if err := n.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
		log.Err(err).Msg("fail to process status message in handling AckMsg")
	}


	return nil
}

// This is helper function to reduce the cognitive complexity
func (n *node)helper(msg types.Message, dst string) {
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dst, 0)
	newPkt := transport.Packet{
		Header: &header,
		Msg:    n.getMarshalledMsg(msg),
	}
	if err := n.conf.Socket.Send(dst, newPkt, sendTimeout); err != nil {
		log.Err(err).Msg("fail to send status message")
	}
}

/*
This function will be called when a StatusMessage is received.
When a peer P receives a status message from a remote peer it compares the StatusMessage to its own view.
There are four possible cases:
1. The remote peer has Rumors that the peer P doesn’t have. 
Actions: The peer P must send a status message to the remote peer
2. The peer P has Rumors that the remote peer doesn’t have.
Actions: The peer P must send all the missing Rumors
3. Both peers have new messages. 
Actions: do case 1 and case 2
4. Both peers have the same view. 
Actions: do ContinueMongering
*/
func (n *node) handleStatusMessage(msg types.Message, pkt transport.Packet) error {
	n.rumorLock.Lock()
	defer n.rumorLock.Unlock()

	remoteStatus := *msg.(*types.StatusMessage)
	HasNew := false
	// diff represents the difference between current node and remote peer
	var diff []types.Rumor

	remoteMsgCnt := 0
	// iterate from current status map to get the difference
	// check the sequence of the current node and remote peer
	// if remote peer has a large sequence, it stands case 1
	// if remote peer has a small sequence, it stands case 2
	for origin, localSeq := range n.status {
		remoteSeq, ok := remoteStatus[origin]
		if ok {
			remoteMsgCnt++
		}
		if remoteSeq < localSeq {
			diff = append(diff, n.rumorsLog[origin][remoteSeq:]...)
		} else if remoteSeq > localSeq {
			HasNew = true
		}
	}

	if len(remoteStatus) > remoteMsgCnt {
		HasNew = true
	}
	
	// case 1
	if HasNew {
		n.helper((*types.StatusMessage)(&n.status), pkt.Header.Source)
	}
	
	// case 2
	if diff != nil {
		n.helper(&types.RumorsMessage{Rumors: diff}, pkt.Header.Source)
	}

	// case 4
	if !HasNew && diff == nil  {
		// not have new neighbour, return
		if n.neighbors.len() == 0 || n.neighbors.hasOnlyNeighbour(pkt.Header.Source) {
			return nil
		}

		// implement ContinueMongering
		if rand.Float64() < n.conf.ContinueMongering {
			// send a status message to a random neighbor
			neighbor := n.neighbors.getNewRandom(pkt.Header.Source)
			n.helper((*types.StatusMessage)(&n.status), neighbor)
		}
	}

	return nil
}

// This function will be called when a EmptyMessage is received.
// Do nothing
func (n *node) handleEmptyMessage(msg types.Message, pkt transport.Packet) error {
	return nil
}

/*
// This function will be called when a PrivateMessage is received.
// Main functionality includes
// 1. Check if the peer’s socket address is in the list of recipients.
// 2. If the previous condition is true, process the embedded message using the message registry.
*/
func (n *node) handlePrivateMessage(msg types.Message, pkt transport.Packet) error {
	privateMsg,ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// check if the peer’s socket address is in the list of recipients
	if _, ok := privateMsg.Recipients[n.conf.Socket.GetAddress()]; ok {
		newPkt := transport.Packet{
			Header: pkt.Header,
			Msg: privateMsg.Msg,
		}
		err := n.conf.MessageRegistry.ProcessPacket(newPkt)
		if err != nil {
			log.Err(err).Msg("failed to process private message packet")
			return err
		}
	}
	return nil
}

