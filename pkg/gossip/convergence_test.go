// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip/simulation"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestConvergence verifies a 10 node gossip network converges within
// a fixed number of simulation cycles. It's really difficult to
// determine the right number for cycles because different things can
// happen during a single cycle, depending on how much CPU time is
// available. Eliminating this variability by getting more
// synchronization primitives in place for the simulation is possible,
// though two attempts so far have introduced more complexity into the
// actual production gossip code than seems worthwhile for a unittest.
func TestConvergence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()

	network := simulation.NewNetwork(stopper, 10, true)

	const maxCycles = 100
	if connectedCycle := network.RunUntilFullyConnected(); connectedCycle > maxCycles {
		t.Errorf("expected a fully-connected network within %d cycles; took %d",
			maxCycles, connectedCycle)
	}
}

// TODO: De-dupe code
func TestConvergenceLarge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	//for numNodes := 1; numNodes <= 32; numNodes++ {
	//for numNodes := 32; numNodes <= 32; numNodes++ {
	for numNodes := 64; numNodes <= 64; numNodes++ {
		t.Run(fmt.Sprintf("%d", numNodes), func(t *testing.T) {
			fmt.Printf("RUNNING WITH NUMNODES=%d\n", numNodes)
			stopper := stop.NewStopper()
			defer stopper.Stop()
			network := simulation.NewNetwork(stopper, numNodes, true)

			const maxCycles = 100
			//connectedCycles := network.RunUntilFullyConnected()
			resultChan := make(chan int64)
			go func() {
				var cyclesRun int64
				network.SimulateNetwork(func(cycle int, network *simulation.Network) bool {
					cyclesRun++
					return cycle < 100
				})
				resultChan <- cyclesRun
			}()

			var cyclesRun int64
			select {
			case cyclesRun = <-resultChan:
			case <-time.After(90 * time.Second):
				var connsRefused int64
				for _, node := range network.Nodes {
					connsRefused += node.Gossip.GetNodeMetrics().ConnectionsRefused.Count()
				}
				t.Fatalf("numNodes=%d timed out with %d connections refused", numNodes, connsRefused)
			}

			var connsRefused int64
			for i, node := range network.Nodes {
				t.Errorf("NUM_NODES: %d,\tNODE_IDX: %d,\tINCOMING: %d,\tOUTGOING: %d", numNodes, i, node.Gossip.GetIncoming().Value(), node.Gossip.GetOutgoing().Value())
				connsRefused += node.Gossip.GetNodeMetrics().ConnectionsRefused.Count()
			}
			t.Errorf("NUM_NODES: %d\tIS_CONNECTED: %v,\tNUM_REFUSED: %d", numNodes, network.IsNetworkConnected(), connsRefused)
			if cyclesRun > maxCycles {
				t.Errorf("expected a fully-connected network within %d cycles; took %d",
					maxCycles, cyclesRun)
			}
		})
	}
}
