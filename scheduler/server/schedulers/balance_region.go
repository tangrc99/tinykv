// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type SuitableStores []*core.StoreInfo

func (s SuitableStores) Len() int {
	return len(s)
}

func (s SuitableStores) Less(i, j int) bool {
	return s[i].GetRegionSize() < s[j].GetRegionSize()
}

func (s SuitableStores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	// find suitable stores
	var suitableStores SuitableStores
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}

	// unable to schedule
	if len(suitableStores) <= 1 {
		return nil
	}

	// sort the suitable stores by region size
	sort.Sort(suitableStores)

	var victim, target *core.StoreInfo
	var victimRegion *core.RegionInfo

	// find the victim region and store
	for i := len(suitableStores) - 1; i >= 0; i-- {
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(regions core.RegionsContainer) {
			victimRegion = regions.RandomRegion(nil, nil)
		})
		if victimRegion != nil {
			victim = suitableStores[i]
			break
		}
		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(regions core.RegionsContainer) {
			victimRegion = regions.RandomRegion(nil, nil)
		})
		if victimRegion != nil {
			victim = suitableStores[i]
			break
		}
		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(regions core.RegionsContainer) {
			victimRegion = regions.RandomRegion(nil, nil)
		})
		if victimRegion != nil {
			victim = suitableStores[i]
			break
		}
	}

	// failed to find a region
	if victimRegion == nil || len(victimRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}

	storeIds := victimRegion.GetStoreIds()
	// try to get the target region
	for i := 0; i < len(suitableStores); i++ {
		if _, exist := storeIds[suitableStores[i].GetID()]; !exist {
			target = suitableStores[i]
			break
		}
	}
	// failed to get target region
	if target == nil {
		return nil
	}

	// The scheduling threshold is not met
	if victim.GetRegionSize()-target.GetRegionSize() <= 2*victimRegion.GetApproximateSize() {
		return nil
	}

	peer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		return nil
	}

	peerOperator, err := operator.CreateMovePeerOperator(s.GetType(), cluster, victimRegion, operator.OpBalance, victim.GetID(), target.GetID(), peer.Id)
	if err != nil {
		return nil
	}

	return peerOperator
}
