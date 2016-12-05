// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package storage_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop()

	errors := make(chan error)
	descs := make(chan *roachpb.RangeDescriptor)
	unregister := tc.Servers[0].Gossip().RegisterCallback(gossip.KeyFirstRangeDescriptor,
		func(_ string, content roachpb.Value) {
			var desc roachpb.RangeDescriptor
			if err := content.GetProto(&desc); err != nil {
				errors <- err
			} else {
				descs <- &desc
			}
		},
	)
	// Unregister the callback before attempting to stop the stopper to prevent
	// deadlock. This is still flaky in theory since a callback can fire between
	// the last read from the channels and this unregister, but testing has
	// shown this solution to be sufficiently robust for now.
	defer unregister()

	// Wait for the specified descriptor to be gossiped for the first range. We
	// loop because the timing of replica addition and lease transfer can cause
	// extra gossiping of the first range.
	waitForGossip := func(desc roachpb.RangeDescriptor) {
		for {
			select {
			case err := <-errors:
				t.Fatal(err)
			case gossiped := <-descs:
				if reflect.DeepEqual(&desc, gossiped) {
					return
				}
				log.Infof(context.TODO(), "expected\n%+v\nbut found\n%+v", desc, gossiped)
			}
		}
	}

	// Expect an initial callback of the first range descriptor.
	select {
	case err := <-errors:
		t.Fatal(err)
	case <-descs:
	}

	// Add two replicas. The first range descriptor should be gossiped after each
	// addition.
	var desc roachpb.RangeDescriptor
	firstRangeKey := keys.MinKey
	for i := 1; i <= 2; i++ {
		var err error
		if desc, err = tc.AddReplicas(firstRangeKey, tc.Target(i)); err != nil {
			t.Fatal(err)
		}
		waitForGossip(desc)
	}

	// Transfer the lease to a new node. This should cause the first range to be
	// gossiped again.
	if err := tc.TransferRangeLease(desc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// Remove a non-lease holder replica.
	desc, err := tc.RemoveReplicas(firstRangeKey, tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// TODO(peter): Re-enable or remove when we've resolved the discussion
	// about removing the lease-holder replica. See #7872.

	// // Remove the lease holder replica.
	// leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	// desc, err = tc.RemoveReplicas(firstRangeKey, leaseHolder)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// select {
	// case err := <-errors:
	// 	t.Fatal(err)
	// case gossiped := <-descs:
	// 	if !reflect.DeepEqual(desc, gossiped) {
	// 		t.Fatalf("expected\n%+v\nbut found\n%+v", desc, gossiped)
	// 	}
	// }
}

// TestGossipHandlesReplacedNode tests that we can shut down a node and
// replace it with a new node at the same address (simulating a node getting
// restarted after losing its data) without the cluster breaking.
func TestGossipHandlesReplacedNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	start := time.Now()

	// Shorten the raft tick interval and election timeout to make range leases
	// much shorter than normal. This keeps us from having to wait so long for
	// the replaced node's leases to time out.
	raftTickInterval := 20 * time.Millisecond
	raftElectionTimeoutTicks := 10

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				RaftTickInterval:         raftTickInterval,
				RaftElectionTimeoutTicks: raftElectionTimeoutTicks,
			},
		})
	log.Infof(ctx, "started test cluster: %v", time.Since(start))
	defer tc.Stopper().Stop()

	// Take down the first node of the cluster and replace it with a new one.
	// We replace the first node rather than the second or third to be adversarial
	// because it typically has the most leases on it.
	oldNodeIdx := 0
	newServerArgs := base.TestServerArgs{
		Addr:                     tc.Servers[oldNodeIdx].ServingAddr(),
		PartOfCluster:            true,
		JoinAddr:                 tc.Servers[1].ServingAddr(),
		RaftTickInterval:         raftTickInterval,
		RaftElectionTimeoutTicks: raftElectionTimeoutTicks,
	}
	tc.StopServer(oldNodeIdx)
	log.Infof(ctx, "stopped server: %v", time.Since(start))
	//newTime := tc.Server(1).Clock().Now().Add(int64(10*time.Second), 0)
	//tc.Server(1).Clock().Update(newTime)
	//tc.Server(2).Clock().Update(newTime)
	tc.AddServer(t, newServerArgs)
	log.Infof(ctx, "added server: %v", time.Since(start))
	tc.WaitForStores(t, tc.Server(1).Gossip())
	log.Infof(ctx, "waited for stores: %v", time.Since(start))

	// Ensure that all servers still running are responsive. If the two remaining
	// original nodes don't refresh their connection to the address of the first
	// node, they can get stuck here.
	for i := 1; i < 4; i++ {
		kvClient := tc.Server(i).KVClient().(*client.DB)
		log.Infof(ctx, "got kvclient %d: %v", i, time.Since(start))
		if err := kvClient.Put(ctx, fmt.Sprintf("%d", i), i); err != nil {
			t.Errorf("failed Put to node %d: %s", i, err)
		}
		log.Infof(ctx, "finished put %d: %v", i, time.Since(start))
	}
	log.Infof(ctx, "done: %v", time.Since(start))
}
