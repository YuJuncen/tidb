// Copyright 2015 PingCAP, Inc.
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

package util

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/pd/pkg/tsoutil"
	"go.uber.org/zap"
)

const (
	defaultCheckVersInterval = 20 * time.Millisecond
	bulkCheckVersInterval    = 2 * time.Millisecond

	defaultCheckVersFirstWaitTime = 50 * time.Millisecond
	bulkCheckVersFirstWaitTime    = 3 * time.Millisecond
)

var (
	globalKeeperOnce                    = new(sync.Once)
	globalKeeper     *BulkDDLModeKeeper = nil
)

func GlobalBulkDDLKeeper() *BulkDDLModeKeeper {
	globalKeeperOnce.Do(func() {
		globalKeeper = newBulkDDLModeKeeper()
	})
	return globalKeeper
}

type BulkDDLModeKeeper struct {
	disableAt chan<- time.Time
}

func timerAt(t time.Time) *time.Timer {
	now := time.Now()
	diff := t.Sub(now)
	if diff < 0 {
		logutil.BgLogger().Warn("timerAt try to create a timer before the current time.",
			zap.Stringer("now", now),
			zap.Stringer("target", t),
		)
		return nil
	}
	return time.NewTimer(diff)
}

type resetableTimer struct {
	next  time.Time
	timer *time.Timer
}

func (r *resetableTimer) C() <-chan time.Time {
	if r.timer == nil {
		return nil
	}
	return r.timer.C
}

func (r *resetableTimer) ResetAt(t time.Time) {
	r.timer.Stop()
	r.timer = timerAt(t)
	r.next = t
}

func (r *resetableTimer) Stop() {
	r.timer.Stop()
	r.timer = nil
	r.next = time.Now()
}

func newResetableTimer() *resetableTimer {
	return &resetableTimer{
		next:  time.Now(),
		timer: nil,
	}
}

func newBulkDDLModeKeeper() *BulkDDLModeKeeper {
	notify := make(chan time.Time, 4)
	b := &BulkDDLModeKeeper{
		disableAt: notify,
	}
	b.forkDisableWatcher(notify)
	return b
}

func (b *BulkDDLModeKeeper) forkDisableWatcher(notify <-chan time.Time) {
	timer := newResetableTimer()
	go func() {
		defer b.disable()
		for {
			select {
			case <-timer.C():
				b.disable()
				timer.Stop()
			case nextAt := <-notify:
				if nextAt.After(timer.next) {
					timer.ResetAt(nextAt)
				}
			}
		}
	}()
}

// EnableUntil enable the bulk ddl mode until a tso.
func (b *BulkDDLModeKeeper) EnableUntil(ts uint64) {
	till, _ := tsoutil.ParseTS(ts)
	now := time.Now()
	if now.After(till) {
		return
	}
	b.enable()
	b.disableAt <- till
}

func (b *BulkDDLModeKeeper) enable() {
	atomic.StoreInt64(&CheckVersFirstWaitTime, int64(bulkCheckVersFirstWaitTime))
	atomic.StoreInt64(&CheckVersInterval, int64(bulkCheckVersInterval))
}

func (b *BulkDDLModeKeeper) disable() {
	atomic.StoreInt64(&CheckVersFirstWaitTime, int64(defaultCheckVersFirstWaitTime))
	atomic.StoreInt64(&CheckVersInterval, int64(defaultCheckVersInterval))
}
