package impl

import (
	"math/rand"
	"sync"
)

// Struct storing the node neighbours
type neighborMap struct {
	sync.RWMutex
	nei []string
}

// Return the number of neighbours
func (ns *neighborMap) len() int {
	ns.Lock()
	defer ns.Unlock()

	return len(ns.nei)
}

// Add a neighbour
func (ns *neighborMap) add(neighbor string) {
	ns.Lock()
	defer ns.Unlock()

	ns.nei = append(ns.nei, neighbor)
}

// Delete a neighbour
func (ns *neighborMap) delete(neighbor string) {
	ns.Lock()
	defer ns.Unlock()

	for i, v := range ns.nei {
		if v == neighbor {
			ns.nei = append(ns.nei[:i], ns.nei[i+1:]...)
			return
		}
	}
}

// Get a random neighbour
func (ns *neighborMap) getRandom() string {
	ns.RLock()
	defer ns.RUnlock()

	return ns.nei[rand.Intn(len(ns.nei))]
}

// Get a new random neighbour except the neighbour specified
func (ns *neighborMap) getNewRandom(oldNeighbor string) string {
	ns.RLock()
	defer ns.RUnlock()

	for {
		neighbor := ns.nei[rand.Intn(len(ns.nei))]
		if neighbor != oldNeighbor {
			return neighbor
		}
	}
}

// Check if the node has only one specified node
func (ns *neighborMap) hasOnlyNeighbour(neighbor string) bool {
	ns.Lock()
	defer ns.Unlock()

	return len(ns.nei) == 1 && ns.nei[0] == neighbor
}

// Check if the node has a specific neighbour
func (ns *neighborMap) has(neighbor string) bool {
	ns.Lock()
	defer ns.Unlock()

	for _, v := range ns.nei {
		if v == neighbor {
			return true
		}
	}
	return false
}
