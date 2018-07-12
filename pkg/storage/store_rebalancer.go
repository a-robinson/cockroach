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

package storage

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const (
	// storeRebalancerTimerDuration is how frequently to check the store-level
	// balance of the cluster.
	storeRebalancerTimerDuration = time.Minute

	// minQPSThresholdDifference is the minimum QPS difference from the cluster
	// mean that this system should care about. In other words, we won't worry
	// about rebalancing for QPS reasons if a store's QPS differs from the mean
	// by less than this amount even if the amount is greater than the percentage
	// threshold. This avoids too many lease transfers in lightly loaded clusters.
	minQPSThresholdDifference = 100
)

type StoreRebalancer struct {
	log.AmbientContext
	store     *Store // TODO: Switch this to an interface?
	st        *cluster.Settings
	rq        *replicateQueue // TODO: factor the important bits out?
	allocator Allocator
}

func NewStoreRebalancer(
	ambientCtx log.AmbientContext, store *Store, st *cluster.Settings, allocator Allocator,
) *StoreRebalancer {
	ambientCtx.AddLogTag("store-rebalancer", nil)
	return &StoreRebalancer{
		AmbientContext: ambientCtx,
		store:          store,
		st:             st,
		allocator:      allocator,
	}
}

// Start runs an infinite loop in a goroutine which regularly checks whether
// the store is overloaded along any important dimension (e.g. range count,
// QPS, disk usage), and if so attempts to correct that by moving leases or
// replicas elsewhere.
//
// This worker acts on store-level imbalances, whereas the replicate queue
// makes decisions based on the zone config constraints and diversity of
// individual ranges. This means that there are two different workers that
// could potentially be making decisions about a given range, so they have to
// be careful to avoid stepping on each others' toes.
// TODO(a-robinson): Figure out how to expose metrics from this.
// TODO(a-robinson): Make sure this is easily debuggable.
func (s *StoreRebalancer) Start(ctx context.Context, stopper *stop.Stopper) {
	ctx = s.AnnotateCtx(ctx)

	// Start a goroutine that watches and proactively renews certain
	// expiration-based leases.
	stopper.RunWorker(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			// Wait out the first tick before doing anything since the store is still
			// starting up and we might as well wait for some qps/wps stats to
			// accumulate.
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ticker.C:
			}

			if !EnableStatsBasedRebalancing.Get(&s.st.SV) {
				continue
			}

			localDesc, found := s.allocator.storePool.getStoreDescriptor(s.store.StoreID())
			if !found {
				continue
			}
			storelist, _, _ := s.allocator.storePool.getStoreList(roachpb.RangeID(0), storeFilterNone)
			s.rebalanceStore(ctx, localDesc, storelist)
		}
	})
}

func (s *StoreRebalancer) rebalanceStore(
	ctx context.Context, localDesc roachpb.StoreDescriptor, storelist StoreList,
) {
	statThreshold := statRebalanceThreshold.Get(&s.st.SV)

	// First check if we should transfer leases away to better balance QPS.
	qpsMinThreshold := math.Min(storelist.candidateQueriesPerSecond.mean*(1-statThreshold),
		storelist.candidateQueriesPerSecond.mean-minQPSThresholdDifference)
	qpsMaxThreshold := math.Max(storelist.candidateQueriesPerSecond.mean*(1+statThreshold),
		storelist.candidateQueriesPerSecond.mean+minQPSThresholdDifference)
	// TODO: Do this in a loop?
	if localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		repl, target := s.chooseLeaseToTransfer(localDesc, storelist, qpsMinThreshold, qpsMaxThreshold)
		if repl != nil {
			if err := s.rq.transferLease(ctx, repl, target); err != nil {
				log.Errorf(ctx, "%s: unable to transfer lease to s%d: %v", repl, target.StoreID, err)
				return // TODO: Break out of lease transfer loop instead?
			}
		}
	}
}

// TODO: Pick replica to move and store to move it to - need something tracking the hot/cold replicas?
// TODO: How to account for follow-the-sun in this process?
func (s *StoreRebalancer) chooseLeaseToTransfer(
	localDesc roachpb.StoreDescriptor, storelist StoreList, minQPS float64, maxQPS float64,
) (*Replica, roachpb.ReplicaDescriptor) {
	return nil, roachpb.ReplicaDescriptor{}
}
