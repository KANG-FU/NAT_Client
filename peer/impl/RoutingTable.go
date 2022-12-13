package impl

import (
	"sync"
)


// The routing table is used to get the next-hop of a packet based on its destination. It should
// be used each time a peer receives a packet on its socket, and updated with the AddPeer
// and SetRoutingEntry functions.
// A routing table can be constructed by map, where key is the original node and the value is the relay address.
// A routing table always has an entry of itself
//	Table[myAddr] = myAddr.
//  Table[C] = B means that to reach C, message must be sent to B to relay.
type RoutingTable struct {
	route map[string]string
	sync.RWMutex
}

func NewConcurrentMapString() RoutingTable {
	return RoutingTable{
		route: make(map[string]string),
	}
}

// Output the whole Routing Table
func (RT *RoutingTable) GetRoutingTable() map[string]string {
	RT.Lock()
	defer RT.Unlock()

	return RT.route
}

// Add a record to Routing Table
func (RT *RoutingTable) Add(key, value string) {
	RT.Lock()
	defer RT.Unlock()

	RT.route[key] = value
}

// if key does not exist, return "" and the default value for map is ""
func (RT *RoutingTable) Get(key string) string {
	RT.Lock()
	defer RT.Unlock()

	return RT.route[key]
}

// Delete a record to Routing Table
func (RT *RoutingTable) Delete(key string) {
	RT.Lock()
	defer RT.Unlock()

	delete(RT.route, key)
}


// SetRoutingEntry sets the routing entry. Overwrites it if the entry already exists. 
// If the origin is equal to the relayAddr, then the node has a new neighbor.
// If relayAddr is empty then the record must be deleted (and the peer has potentially lost a neighbor).
func (RT *RoutingTable) SetRoutingEntry(origin, relayAddr string) {
	RT.Lock()
	defer RT.Unlock()

	if relayAddr == "" {
		delete(RT.route, origin)
		return
	}

	if relay, exists := RT.route[origin]; exists {
		if relay == origin {
			return
		}
	}

	RT.route[origin] = relayAddr
}



