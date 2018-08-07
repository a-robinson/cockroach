// Copyright 2018 The Cockroach Authors.
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

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var (
	// noLocalityStores specifies a set of stores where one store is
	// under-utilized in terms of QPS, three are in the middle, and one is
	// over-utilized.
	noLocalityStores = []*roachpb.StoreDescriptor{
		{
			StoreID: 1,
			Node:    roachpb.NodeDescriptor{NodeID: 1},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 500,
			},
		},
		{
			StoreID: 2,
			Node:    roachpb.NodeDescriptor{NodeID: 2},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 900,
			},
		},
		{
			StoreID: 3,
			Node:    roachpb.NodeDescriptor{NodeID: 3},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1000,
			},
		},
		{
			StoreID: 4,
			Node:    roachpb.NodeDescriptor{NodeID: 4},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1100,
			},
		},
		{
			StoreID: 5,
			Node:    roachpb.NodeDescriptor{NodeID: 5},
			Capacity: roachpb.StoreCapacity{
				QueriesPerSecond: 1500,
			},
		},
	}
)

type testRanges struct {
	rangeID roachpb.RangeID
	// The first storeID in the list will be the leaseholder.
	storeIDs []roachpb.StoreID
	qps      float64
}

func loadRanges(rr *replicaRankings, ranges []testRanges) {
	acc := rr.newAccumulator()
	for _, r := range ranges {
		repl := &Replica{RangeID: r.rangeID}
		repl.mu.state.Desc.RangeID = r.rangeID
		for _, storeID := range r.storeIDs {
			repl.mu.state.Desc.Replicas = append(repl.mu.state.Desc.Replicas, roachpb.ReplicaDescriptor{
				NodeID:    roachpb.NodeID(storeID),
				StoreID:   storeID,
				ReplicaID: roachpb.ReplicaID(storeID),
			})
		}
		repl.mu.state.Lease = &roachpb.Lease{
			Expiration: &hlc.MaxTimestamp,
			Replica:    repl.mu.state.Desc.Replicas[0],
		}
		acc.addReplica(replicaWithStats{
			repl: repl,
			qps:  r.qps,
		})
	}
	rr.update(acc)
}

func TestChooseLeaseToTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	stopper, g, _, a, _ := createTestAllocator( /* deterministic */ false)
	defer stopper.Stop(context.Background())
	gossiputil.NewStoreGossiper(g).GossipStores(noLocalityStores, t)

	cfg := TestStoreConfig(nil)
	s := createTestStoreWithoutStart(t, stopper, &cfg)
	rq := newReplicateQueue(s, g, a)
	localDesc := noLocalityStores[0]
	rr := newReplicaRankings()

	sr := NewStoreRebalancer(cfg.AmbientCtx, cfg.Settings, rq, rr)

	loadRanges(rr, []testRanges{
		{roachpb.RangeID(1), []roachpb.StoreID{1, 2, 3}, 100},
		{roachpb.RangeID(2), []roachpb.StoreID{1, 2, 3}, 1000},
	})

	replWithStats, target := sr.chooseLeaseToTransfer(
		ctx, localDesc, storelist, storeMap, minQPS, maxQPS)
}
