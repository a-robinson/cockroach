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
				log.Warningf(ctx, "StorePool missing descriptor for local store")
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

	for localDesc.Capacity.QueriesPerSecond > qpsMaxThreshold {
		log.Infof(ctx, "considering load-based lease transfers for s%d with %.2f qps (mean=%.2f, upperThreshold=%2.f)",
			localDesc.StoreID, localDesc.Capacity.QueriesPerSecond, storelist.candidateQueriesPerSecond.mean, qpsMaxThreshold)
		storeMap := storeListToMap(storelist)
		replWithStats, target := s.chooseLeaseToTransfer(
			ctx, localDesc, storelist, storeMap, qpsMinThreshold, qpsMaxThreshold)
		if replWithStats.repl == nil {
			break
		}
		if err := s.rq.transferLease(ctx, replWithStats.repl, target); err != nil {
			log.Errorf(ctx, "%s: unable to transfer lease to s%d: %v",
				replWithStats.repl, target.StoreID, err)
			return
		}
		localDesc.Capacity.QueriesPerSecond -= replWithStats.qps
	}
}

func (s *StoreRebalancer) chooseLeaseToTransfer(
	ctx context.Context,
	localDesc roachpb.StoreDescriptor,
	storelist StoreList,
	storeMap map[roachpb.StoreID]*roachpb.StoreDescriptor,
	minQPS float64,
	maxQPS float64,
) (replicaWithStats, roachpb.ReplicaDescriptor) {
	sysCfg, cfgOk := s.store.cfg.Gossip.GetSystemConfig()
	if !cfgOk {
		// TODO(a-robinson): Should we just ignore lease preferences when the system
		// config is unavailable rather than disabling rebalance lease transfers?
		log.VEventf(ctx, 1, "no system config available, unable to choose a lease transfer target")
		return replicaWithStats{}, roachpb.ReplicaDescriptor{}
	}

	for {
		// TODO(a-robinson): Should we take the number of leases on each store into
		// account here?
		replWithStats := s.store.replRankings.topQPS()
		if replWithStats.repl == nil {
			return replicaWithStats{}, roachpb.ReplicaDescriptor{}
		}
		desc := replWithStats.repl.Desc()
		log.VEventf(ctx, 3, "considering lease transfer for r%d with %.2f qps", desc.RangeID, replWithStats.qps)
		for _, candidate := range desc.Replicas {
			storeDesc, ok := storeMap[candidate.StoreID]
			if !ok {
				log.VEventf(ctx, 3, "missing store descriptor for s%d", candidate.StoreID)
				continue
			}
			if storeDesc.Capacity.QueriesPerSecond+replWithStats.qps >= storelist.candidateQueriesPerSecond.mean {
				log.VEventf(ctx, 3, "QPS for r%d would push s%d over the mean (%.2f)",
					desc.RangeID, candidate.StoreID, storeDesc.Capacity.QueriesPerSecond+replWithStats.qps)
				continue
			}
			zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
			if err != nil {
				log.Error(ctx, err)
				return replicaWithStats{}, roachpb.ReplicaDescriptor{}
			}
			preferred := s.allocator.preferredLeaseholders(zone, desc.Replicas)
			if len(preferred) > 0 && !storeHasReplica(candidate.StoreID, preferred) {
				log.VEventf(ctx, 3, "s%d not a preferred leaseholder; preferred: %v", candidate.StoreID, preferred)
				continue
			}
			filteredStorelist := storelist.filter(zone.Constraints)
			if s.allocator.followTheWorkloadPrefersLocal(
				ctx,
				filteredStorelist,
				localDesc,
				candidate.StoreID,
				desc.Replicas,
				replWithStats.repl.leaseholderStats,
			) {
				log.VEventf(ctx, 3, "r%d is on s%d due to follow-the-workload; skipping",
					desc.RangeID, localDesc.StoreID)
				continue
			}
			return replWithStats, candidate
		}
	}

	return replicaWithStats{}, roachpb.ReplicaDescriptor{}
}

func storeListToMap(sl StoreList) map[roachpb.StoreID]*roachpb.StoreDescriptor {
	storeMap := make(map[roachpb.StoreID]*roachpb.StoreDescriptor)
	for i := range sl.stores {
		storeMap[sl.stores[i].StoreID] = &sl.stores[i]
	}
	return storeMap
}
