package impl

import (
	"errors"
	"fmt"
	"math/rand"

	// "math/rand"

	// "math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

const sendTimeout = 500 * time.Millisecond
const serverIP = "192.168.137.1"


// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	node := &node{
		conf:          conf,
		RoutingTable:  NewConcurrentMapString(),
		rumorSequence: 0,
		status:        make(map[string]uint),
		rumorsLog:     make(map[string][]types.Rumor),
		port:          0,
	}

	// initialization. Add itself to routing table
	node.AddPeer(conf.Socket.GetAddress())
	node.AddPeer(serverIP)
	return node
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer

	// You probably want to keep the peer.Configuration on this struct:
	conf         peer.Configuration
	RoutingTable RoutingTable
	// store the neighbour of node
	neighbors neighborMap
	// concurrent map for storing the packetID
	ack           sync.Map
	rumorSequence uint32
	rumorLock     sync.Mutex
	// the last known sequence for an peer
	status map[string]uint
	// all the rumors stored
	rumorsLog map[string][]types.Rumor

	port int
}

const defaultLevel = zerolog.NoLevel

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

var Logger = zerolog.New(logout).Level(defaultLevel).
	With().Timestamp().Logger().
	With().Caller().Logger()

// Start implements peer.Service
func (n *node) Start() error {

	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.handleChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.handleEmptyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.handlePrivateMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.handleRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.handleStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.handleAckMessage)

	go n.Listen()

	if n.conf.HeartbeatInterval > 0 {
		go n.heartbeat()
	}
	if n.conf.AntiEntropyInterval > 0 {
		go n.AntiEntropy()
	}

	n.register(serverIP)

	return nil
	// panic("to be implemented in HW0")
}

func (n *node) register(serverRegIP string) {
	rand.Seed(time.Now().UnixNano())
	n.port = rand.Intn(3000) + 3000
	fmt.Println(n.port)
    localAddr := net.UDPAddr{Port: n.port}
	// localAddr, err := net.ResolveUDPAddr("udp", n.conf.Socket.GetAddress())
	// if err != nil {
	// 	log.Err(err).Msgf("failed to resolve UDP address %s", localAddr)
	// 	return 
	// }
	remoteAddr := net.UDPAddr{
		IP:   net.ParseIP(serverIP),
		Port: 8081,
	}
	fmt.Println("connect to server")
	conn, err := net.DialUDP("udp", &localAddr, &remoteAddr)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("register message to server")
	conn.Write([]byte("This is peer: " + n.conf.Socket.GetAddress()))
	buf := make([]byte, 256)
	l, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return
	}
	fmt.Printf("%s \n", buf[:l])
	conn.Close()

	// header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), serverRegIP, 0)
	// pkt := transport.Packet{
	// 	Header: &header,
	// 	Msg:    transport.Message{

	// 	}
	// 	types.ChatMessage{Message :"This is peer: " + n.conf.Socket.GetAddress()},
	// }
	// err := n.conf.Socket.Send(serverRegIP, pkt, sendTimeout)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// 	log.Err(err).Msg("errors: failed to send unicast message")
	// 	return 
	// }

}

// func(n *node) register(serverIP string) {
// 	chat := types.ChatMessage{
// 		Message: "this is my IP address:" + n.conf.Socket.GetAddress(),
// 	}
// 	data, err := json.Marshal(&chat)
// 	if err != nil{
// 		return
// 	}
// 	msg := transport.Message{
// 		Type:    chat.Name(),
// 		Payload: data,
// 	}
// 	n.Unicast(serverIP, msg)
// }

// Stop implements peer.Service
func (n *node) Stop() error {

	// atomic.AddInt32(&n.stop, 1)
	// n.WaitGroup.Wait()
	stopCh := make(chan bool, 1)
	stopCh <- true

	return nil
	// panic("to be implemented in HW0")
}

func (n *node) holePunching(dest string) (string) {
	localAddr := net.UDPAddr{Port: n.port}
	remoteAddr := net.UDPAddr{
		IP:   net.ParseIP(serverIP),
		Port: 8080,
	}
	fmt.Println("connect to server")
	conn, err := net.DialUDP("udp", &localAddr, &remoteAddr)
	if err != nil {
		return ""
	}
	fmt.Println("request message to server")
	conn.Write([]byte(n.conf.Socket.GetAddress() + "->" + dest))

	buf := make([]byte, 256)
	l, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return ""
	}
	conn.Close()
	fmt.Println(string(buf[:l]))
	return string(buf[:l])
	// if strings.Contains(string(buf[:l]), "currently not online"){
	// 	// try a long time after
	// 	return false
	// } else if strings.Contains(string(buf[:l]), "busy"){
	// 	// try a short time after
	// }
}

// Unicast implements peer.Messaging
// check if there is a route to dest from routing table
// if it is, get the nextHop address and send the message
func (n *node) Unicast(dest string, msg transport.Message) error {
	// panic("to be implemented in HW0")
	localAddr := net.UDPAddr{Port: n.port}
	fmt.Println(localAddr.String())
	err := n.unicastLAN(dest, msg)
	if strings.Contains(err.Error(), "unreachable dest") {
		targetAddr := n.holePunching(dest)
		if targetAddr == "" {
			return nil
		}
		header := transport.NewHeader(localAddr.String(), localAddr.String(), targetAddr, 0)
		pkt := transport.Packet{
			Header: &header,
			Msg:    &msg,
		}
		err := n.conf.Socket.Send(targetAddr, pkt, sendTimeout)
		if err != nil {
			fmt.Println(err.Error())
			log.Err(err).Msg("errors: failed to send unicast message")
			return fmt.Errorf("fail to send message")
		}
	} else if strings.Contains(err.Error(), "fail to send message") {
		return fmt.Errorf("fail to send message")
	}
	return nil
}

func (n *node) unicastLAN(dest string, msg transport.Message) error {
	nextHop := n.RoutingTable.Get(dest)
	if nextHop == "" {
		return fmt.Errorf("unreachable dest: %v", dest)
	}

	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), dest, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}

	err := n.conf.Socket.Send(nextHop, pkt, sendTimeout)
	if err != nil {
		log.Err(err).Msg("errors: failed to send unicast message")
		return fmt.Errorf("fail to send message")
	}

	return nil
}

/*
The boardcast functionalities includes
1. Construct the rumor message.
2. Process the message locally
3. Update the node status
4. Choose a random neighbour and send the rumor. If there is no neighbour, return
Before sending the rumor, map the packetID to ack channel so that ack message can be handled properly
5. goroutine to wait for ack
*/
func (n *node) Broadcast(msg transport.Message) error {
	n.rumorLock.Lock()
	// construct the rumor message
	n.rumorSequence++
	rumors := types.RumorsMessage{Rumors: []types.Rumor{
		{
			Origin:   n.conf.Socket.GetAddress(),
			Sequence: uint(n.rumorSequence),
			Msg:      &msg,
		},
	}}
	// Process locally
	header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), 0)
	localMsg := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}
	if err := n.conf.MessageRegistry.ProcessPacket(localMsg); err != nil {
		n.rumorLock.Unlock()
		log.Err(err).Msg("errors: fail to process broadcast packet locally")
		return err
	}
	// update the node status and rumor log
	n.status[n.conf.Socket.GetAddress()]++
	n.rumorsLog[n.conf.Socket.GetAddress()] = append(n.rumorsLog[n.conf.Socket.GetAddress()], rumors.Rumors[0])
	n.rumorLock.Unlock()

	if n.neighbors.len() == 0 {
		return nil
	}

	// choose a random neighbour to send the rumor
	neighbor := n.neighbors.getRandom()
	header = transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    n.getMarshalledMsg(&rumors),
	}

	// channel to wait for ack msg
	// create a map between the packet ID and corresponding chanel
	ackCh := make(chan int, 1)
	n.ack.Store(pkt.Header.PacketID, ackCh)

	// send it
	if err := n.conf.Socket.Send(neighbor, pkt, sendTimeout); err != nil {
		log.Err(err).Msg("errors: can't send the packet for broadcast")
		return err
	}

	// wait for ack. If no ack, send to another neighbor
	go n.waitAck(ackCh, neighbor, pkt)

	return nil
}

func (n *node) waitAck(ackCh chan int, neighbor string, pkt transport.Packet) {
	// no timeout
	if n.conf.AckTimeout == 0 {
		// receive the signal from channel means receiving the ack msg
		<-ackCh
		return
	}

	// with timeout
	for {
		select {
		// receive the signal from channel means receiving the ack msg
		case <-ackCh:
			return
		// timeout happens
		case <-time.After(n.conf.AckTimeout):
			// delete the invalid packetID record
			n.ack.Delete(pkt.Header.PacketID)
			// no new neighbour return
			if n.neighbors.hasOnlyNeighbour(neighbor) {
				return
			}
			// select a new neighbour to send the rumor after the timeout
			neighbor := n.neighbors.getNewRandom(neighbor)
			header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
			pkt.Header = &header

			// channel with capacity
			ackCh = make(chan int, 1)
			n.ack.Store(pkt.Header.PacketID, ackCh)

			if err := n.conf.Socket.Send(neighbor, pkt, sendTimeout); err != nil {
				log.Err(err).Msg("fail to send rumor when broadcasting")
			}
		}
	}
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, a := range addr {
		n.SetRoutingEntry(a, a)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.RoutingTable.GetRoutingTable()
}

// SetRoutingEntry implements peer.Service
// Once set the routing entry, add(delete) the corresponding neighbours
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.RoutingTable.Delete(origin)
		n.neighbors.delete(origin)
	} else {
		n.RoutingTable.Add(origin, relayAddr)
		if origin == relayAddr && origin != n.conf.Socket.GetAddress() {
			n.neighbors.add(origin)
		}
	}
}

// Convert types.Message to *transport.Message.
// Returns nil if marshall failed.
func (n *node) getMarshalledMsg(msg types.Message) *transport.Message {
	m, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		log.Err(err).Msg("failed to marshall message")
		return nil
	}
	return &m
}

/*
The Listen function starts listening on incoming messages with the socket.
With the routing table implemented in the next section,
you must check if the message is for the node or relay it if necessary.
If the packet is for the peer, then the registry must be used to execute the callback.
If the packet is to be relayed, update the RelayedBy field of the packet’s header to the peer’s address.
*/
func (n *node) Listen() {
	stopCh := make(chan bool, 1)
	for {
		select {
		case <-stopCh:
			return
		default:
			pkt, err := n.conf.Socket.Recv(time.Millisecond * 50)
			if errors.Is(err, transport.TimeoutError(0)) {
				continue
			} else if err != nil {
				log.Err(err).Msg("socket recv has error")
			}

			var e error
			if pkt.Header.Destination == n.conf.Socket.GetAddress() {
				e = n.conf.MessageRegistry.ProcessPacket(pkt)
			} else {
				pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
				e = n.conf.Socket.Send(pkt.Header.Destination, pkt, sendTimeout)
			}
			if e != nil {
				log.Info().Err(e)
			}
		}
	}
}

// AntiEntropyInterval is the interval at which the peer sends a status message to a random neighbor.
// 0 means no status messages are sent.
// Default: 0
func (n *node) AntiEntropy() {
	// if no neighbour, block here until it has neighbours
	for n.neighbors.len() == 0 {
		time.Sleep(n.conf.AntiEntropyInterval)
	}

	// choose random neighbour to send status message every fixed interval
	pkt := transport.Packet{}
	for {
		neighbor := n.neighbors.getRandom()
		header := transport.NewHeader(n.conf.Socket.GetAddress(), n.conf.Socket.GetAddress(), neighbor, 0)
		pkt.Header = &header
		n.rumorLock.Lock()
		pkt.Msg = n.getMarshalledMsg((*types.StatusMessage)(&n.status))
		n.rumorLock.Unlock()
		if err := n.conf.Socket.Send(neighbor, pkt, sendTimeout); err != nil {
			log.Err(err).Msg("failed to send anti-entropy status")
		}
		<-time.After(n.conf.AntiEntropyInterval)
	}
}

// Broadcast with EmptyMessage at start time and at regular interval
func (n *node) heartbeat() {
	EmptyMessage := types.EmptyMessage{}
	for {
		err := n.Broadcast(*n.getMarshalledMsg(EmptyMessage))
		if err != nil {
			log.Err(err).Msg("heartbeat failed")
		}
		<-time.After(n.conf.HeartbeatInterval)
	}
}
