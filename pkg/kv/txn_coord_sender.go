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

package kv

import (
	"context"
	"sort"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	opTxnCoordSender = "txn coordinator"
	opHeartbeatLoop  = "heartbeat txn"
)

// maxTxnIntentsBytes is a threshold in bytes for intent spans stored
// on the coordinator during the lifetime of a transaction. Intents
// are included with a transaction on commit or abort, to be cleaned
// up asynchronously. If they exceed this threshold, they're condensed
// to avoid memory blowup both on the coordinator and (critically) on
// the EndTransaction command at the Raft group responsible for the
// transaction record.
var maxTxnIntentsBytes = settings.RegisterIntSetting(
	"kv.transaction.max_intents_bytes",
	"maximum number of bytes used to track write intents in transactions",
	256*1000,
)

// maxTxnRefreshSpansBytes is a threshold in bytes for refresh spans stored
// on the coordinator during the lifetime of a transaction. Refresh spans
// are used for SERIALIZABLE transactions to avoid client restarts.
var maxTxnRefreshSpansBytes = settings.RegisterIntSetting(
	"kv.transaction.max_refresh_spans_bytes",
	"maximum number of bytes used to track refresh spans in serializable transactions",
	256*1000,
)

// txnCoordState represents the state of the transaction coordinator.
// It is an intermediate state which indicates we've finished the
// transaction at the coordinator level and it's no longer legitimate
// for sending requests, even though we don't yet know for sure that
// the transaction record has been aborted / committed.
type txnCoordState int

const (
	_ txnCoordState = iota
	// done indicates the transaction has been completed via end
	// transaction and can no longer be used.
	done
	// aborted indicates the transaction was aborted or abandoned (e.g.
	// from timeout, heartbeat failure, context cancelation, txn abort
	// or restart, etc.)
	aborted
)

// A TxnCoordSender is the production implementation of client.TxnSender. It is
// a Sender which wraps a lower-level Sender (a DistSender) to which it sends
// commands. It works on behalf of the client to keep a transaction's state
// (e.g. intents) and to perform periodic heartbeating of the transaction
// required when necessary.  Unlike other senders, TxnCoordSender is not a
// singleton - an instance is created for every transaction by the
// TxnCoordSenderFactory.
//
// Among the functions it performs are:
// - Heartbeating of the transaction record. Note that heartbeating is done only
// from the root transaction coordinator, in the event that multiple
// coordinators are active (i.e. in a distributed SQL flow).
// - Accumulating intent spans.
// - Attaching intent spans to EndTransaction requests, for intent cleanup.
// - Handles retriable errors by either bumping the transaction's epoch or, in
// case of TransactionAbortedErrors, cleaning up the transaction (in this case,
// the client.Txn is expected to create a new TxnCoordSender instance
// transparently for the higher-level client).
// - Ensures atomic execution for non-transactional (write) batches by transparently
// wrapping them in transactions when the DistSender is forced to split them for
// multiple ranges. For this reason, generally even non-transactional batches
// need to be sent through a TxnCoordSender.
//
// Since it is stateful, the TxnCoordSender needs to understand when a
// transaction is "finished" and the state can be destroyed. As such there's a
// contract that the client.Txn needs obey. Read-only transactions don't matter
// - they're stateless. For the others, once a BeginTransaction is sent by the
// client, the TxnCoordSender considers the transactions completed in the
// following situations:
// - A batch containing an EndTransactions (commit or rollback) succeeds.
// - A batch containing an EndTransaction(commit=false) succeeds or fails. I.e.
// nothing is expected to follow a rollback attempt.
// - A batch returns a TransactionAbortedError. As mentioned above, the client
// is expected to create a new TxnCoordSender for the next transaction attempt.
//
// Note that "1PC" batches (i.e. batches containing both a Begin and an
// EndTransaction) are no exception from the contract - if the batch fails, the
// client is expected to send a rollback (or perform another transaction attempt
// in case of retriable errors).
type TxnCoordSender struct {
	mu struct {
		syncutil.Mutex

		// hbRunning is set if the TxnCoordSender has a heartbeat loop running for
		// the transaction record.
		hbRunning bool

		// meta contains all coordinator state which may be passed between
		// distributed TxnCoordSenders via MetaRelease() and MetaAugment().
		meta roachpb.TxnCoordMeta

		// intentsSizeBytes is the size in bytes of the intent spans in the
		// meta, maintained to efficiently check the threshold.
		intentsSizeBytes int64
		// refreshSpansBytes is the total size in bytes of the spans
		// encountered during this transaction that need to be refreshed to
		// avoid serializable restart.
		refreshSpansBytes int64
		// lastUpdateNanos is the latest wall time in nanos the client sent
		// transaction operations to this coordinator. Accessed and updated
		// atomically.
		lastUpdateNanos int64
		// Analogous to lastUpdateNanos, this is the wall time at which the
		// transaction was instantiated.
		firstUpdateNanos int64
		// txnEnd is closed when the transaction is aborted or committed,
		// terminating the heartbeat loop.
		txnEnd chan struct{}
		// state indicates the state of the transaction coordinator, which
		// may briefly diverge from the state of the transaction record if
		// the coordinator is aborted after a failed heartbeat, but before
		// we've gotten a response with the updated transaction state.
		state txnCoordState
		// onFinishFn is a closure invoked when state changes to done or aborted.
		onFinishFn func(error)
	}

	// A pointer member to the creating factory provides access to
	// immutable factory settings.
	*TxnCoordSenderFactory

	// typ specifies whether this transaction is the top level,
	// or one of potentially many distributed transactions.
	typ client.TxnType
}

var _ client.TxnSender = &TxnCoordSender{}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts      *metric.CounterWithRates
	Commits     *metric.CounterWithRates
	Commits1PC  *metric.CounterWithRates // Commits which finished in a single phase
	AutoRetries *metric.CounterWithRates // Auto retries which avoid client-side restarts
	Durations   *metric.Histogram

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram

	// Counts of restart types.
	RestartsWriteTooOld    *metric.Counter
	RestartsDeleteRange    *metric.Counter
	RestartsSerializable   *metric.Counter
	RestartsPossibleReplay *metric.Counter
}

var (
	metaAbortsRates = metric.Metadata{
		Name: "txn.aborts",
		Help: "Number of aborted KV transactions"}
	metaCommitsRates = metric.Metadata{
		Name: "txn.commits",
		Help: "Number of committed KV transactions (including 1PC)"}
	// NOTE: The 1PC rate is arguably not accurate because it counts batches
	// containing both BeginTransaction and EndTransaction without caring if the
	// DistSender had to split it for touching multiple ranges.
	metaCommits1PCRates = metric.Metadata{
		Name: "txn.commits1PC",
		Help: "Number of committed one-phase KV transactions"}
	metaAutoRetriesRates = metric.Metadata{
		Name: "txn.autoretries",
		Help: "Number of automatic retries to avoid serializable restarts"}
	metaDurationsHistograms = metric.Metadata{
		Name: "txn.durations",
		Help: "KV transaction durations in nanoseconds"}
	metaRestartsHistogram = metric.Metadata{
		Name: "txn.restarts",
		Help: "Number of restarted KV transactions"}
	metaRestartsWriteTooOld = metric.Metadata{
		Name: "txn.restarts.writetooold",
		Help: "Number of restarts due to a concurrent writer committing first"}
	metaRestartsDeleteRange = metric.Metadata{
		Name: "txn.restarts.deleterange",
		Help: "Number of restarts due to a forwarded commit timestamp and a DeleteRange command"}
	metaRestartsSerializable = metric.Metadata{
		Name: "txn.restarts.serializable",
		Help: "Number of restarts due to a forwarded commit timestamp and isolation=SERIALIZABLE"}
	metaRestartsPossibleReplay = metric.Metadata{
		Name: "txn.restarts.possiblereplay",
		Help: "Number of restarts due to possible replays of command batches at the storage layer"}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:                 metric.NewCounterWithRates(metaAbortsRates),
		Commits:                metric.NewCounterWithRates(metaCommitsRates),
		Commits1PC:             metric.NewCounterWithRates(metaCommits1PCRates),
		AutoRetries:            metric.NewCounterWithRates(metaAutoRetriesRates),
		Durations:              metric.NewLatency(metaDurationsHistograms, histogramWindow),
		Restarts:               metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
		RestartsWriteTooOld:    metric.NewCounter(metaRestartsWriteTooOld),
		RestartsDeleteRange:    metric.NewCounter(metaRestartsDeleteRange),
		RestartsSerializable:   metric.NewCounter(metaRestartsSerializable),
		RestartsPossibleReplay: metric.NewCounter(metaRestartsPossibleReplay),
	}
}

// TxnCoordSenderFactory implements client.TxnSenderFactory.
type TxnCoordSenderFactory struct {
	log.AmbientContext

	st                *cluster.Settings
	wrapped           client.Sender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	linearizable      bool // enables linearizable behavior
	stopper           *stop.Stopper
	metrics           TxnMetrics
}

var _ client.TxnSenderFactory = &TxnCoordSenderFactory{}

// NewTxnCoordSenderFactory creates a new TxnCoordSenderFactory. The
// factory creates new instances of TxnCoordSenders.
//
// TODO(spencer): move these settings into a configuration object and
// supply that to each sender.
func NewTxnCoordSenderFactory(
	ambient log.AmbientContext,
	st *cluster.Settings,
	wrapped client.Sender,
	clock *hlc.Clock,
	linearizable bool,
	stopper *stop.Stopper,
	txnMetrics TxnMetrics,
) *TxnCoordSenderFactory {
	return &TxnCoordSenderFactory{
		AmbientContext:    ambient,
		st:                st,
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: base.DefaultHeartbeatInterval,
		linearizable:      linearizable,
		stopper:           stopper,
		metrics:           txnMetrics,
	}
}

// New is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) New(
	typ client.TxnType, txn *roachpb.Transaction,
) client.TxnSender {
	tcs := &TxnCoordSender{
		typ: typ,
		TxnCoordSenderFactory: tcf,
	}
	tcs.mu.meta.RefreshValid = true

	// If a transaction was passed in bind the TxnCoordSender to it.
	// TODO(andrei): Ideally, if a transaction is not passed it, we should take
	// that to mean that a TxnCoordSender is not needed and we should return the
	// wrapped sender directly. However, there are tests that pass nil and still
	// send transactional requests. That's why the TxnCoordSender is still
	// littered with code handling the case where it is not yet bound to a
	// transaction.
	if txn != nil {
		tcs.mu.meta.Txn = txn.Clone()
	}
	return tcs
}

// WrappedSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) WrappedSender() client.Sender {
	return tcf.wrapped
}

// Metrics returns the factory's metrics struct.
func (tcf *TxnCoordSenderFactory) Metrics() TxnMetrics {
	return tcf.metrics
}

// GetMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetMeta() roachpb.TxnCoordMeta {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	// Copy mutable state so access is safe for the caller.
	meta := tc.mu.meta
	meta.Txn = tc.mu.meta.Txn.Clone()
	meta.Intents = append([]roachpb.Span(nil), tc.mu.meta.Intents...)
	if tc.mu.meta.RefreshValid {
		meta.RefreshReads = append([]roachpb.Span(nil), tc.mu.meta.RefreshReads...)
		meta.RefreshWrites = append([]roachpb.Span(nil), tc.mu.meta.RefreshWrites...)
	}
	return meta
}

// AugmentMeta is part of the client.TxnSender interface.
func (tc *TxnCoordSender) AugmentMeta(ctx context.Context, meta roachpb.TxnCoordMeta) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.meta.Txn.ID == (uuid.UUID{}) {
		log.Fatalf(ctx, "cannot AugmentMeta on unbound TxnCoordSender. meta id: %s", meta.Txn.ID)
	}

	// Sanity check: don't combine if the meta is for a different txn ID.
	if tc.mu.meta.Txn.ID != meta.Txn.ID {
		return
	}
	tc.mu.meta.Txn.Update(&meta.Txn)
	// Do not modify existing span slices when copying.
	tc.mu.meta.Intents, _ = roachpb.MergeSpans(
		append(append([]roachpb.Span(nil), tc.mu.meta.Intents...), meta.Intents...),
	)
	if !meta.RefreshValid {
		tc.mu.meta.RefreshValid = false
		tc.mu.meta.RefreshReads = nil
		tc.mu.meta.RefreshWrites = nil
	} else if tc.mu.meta.RefreshValid {
		tc.mu.meta.RefreshReads, _ = roachpb.MergeSpans(
			append(append([]roachpb.Span(nil), tc.mu.meta.RefreshReads...), meta.RefreshReads...),
		)
		tc.mu.meta.RefreshWrites, _ = roachpb.MergeSpans(
			append(append([]roachpb.Span(nil), tc.mu.meta.RefreshWrites...), meta.RefreshWrites...),
		)
	}
	tc.mu.meta.CommandCount += meta.CommandCount

	// Recompute the size of the intents.
	tc.mu.intentsSizeBytes = 0
	for _, i := range tc.mu.meta.Intents {
		tc.mu.intentsSizeBytes += int64(len(i.Key) + len(i.EndKey))
	}
	// Recompute the size of the refreshes.
	tc.mu.refreshSpansBytes = 0
	for _, u := range tc.mu.meta.RefreshReads {
		tc.mu.refreshSpansBytes += int64(len(u.Key) + len(u.EndKey))
	}
	for _, u := range tc.mu.meta.RefreshWrites {
		tc.mu.refreshSpansBytes += int64(len(u.Key) + len(u.EndKey))
	}
}

// OnFinish is part of the client.TxnSender interface.
func (tc *TxnCoordSender) OnFinish(onFinishFn func(error)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.mu.onFinishFn = onFinishFn
}

// Send implements the batch.Sender interface.
//
// Read/write mutating requests have their key or key range added to the
// transaction's interval tree of key ranges for eventual cleanup via resolved
// write intents; they're tagged to an outgoing EndTransaction request, with the
// receiving replica in charge of resolving them.
func (tc *TxnCoordSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ctx = tc.AnnotateCtx(ctx)

	// Start new or pick up active trace. From here on, there's always an active
	// Trace, though its overhead is small unless it's sampled.
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		sp = tc.AmbientContext.Tracer.StartSpan(opTxnCoordSender)
		defer sp.Finish()
		ctx = opentracing.ContextWithSpan(ctx, sp)
	}

	startNS := tc.clock.PhysicalNow()

	if ba.Txn != nil {
		if tc.mu.meta.Txn.ID == (uuid.UUID{}) {
			log.Fatalf(ctx, "cannot send transactional request through unbound TxnCoordSender")
		}

		ctx = log.WithLogTag(ctx, "txn", uuid.ShortStringer(ba.Txn.ID))
		if log.V(2) {
			ctx = log.WithLogTag(ctx, "ts", ba.Txn.Timestamp)
		}

		// If this request is part of a transaction...
		if err := tc.validateTxnForBatch(ctx, &ba); err != nil {
			return nil, roachpb.NewError(err)
		}

		// Associate the txnID with the trace.
		txnIDStr := ba.Txn.ID.String()
		sp.SetBaggageItem("txnID", txnIDStr)

		_, hasBegin := ba.GetArg(roachpb.BeginTransaction)
		if hasBegin {
			// If there's a BeginTransaction, we need to start the heartbeat loop.
			// Perhaps surprisingly, this needs to be done even if the batch has both
			// a BeginTransaction and an EndTransaction. Although on batch success the
			// heartbeat loop will be stopped right away, on retriable errors we need the
			// heartbeat loop: the intents and txn record should be kept in place just
			// like for non-1PC txns.
			if err := tc.startHeartbeatLoop(ctx); err != nil {
				return nil, roachpb.NewError(err)
			}
		}

		var et *roachpb.EndTransactionRequest
		var hasET bool
		{
			var rArgs roachpb.Request
			rArgs, hasET = ba.GetArg(roachpb.EndTransaction)
			if hasET {
				et = rArgs.(*roachpb.EndTransactionRequest)
				if len(et.Key) != 0 {
					return nil, roachpb.NewErrorf("EndTransaction must not have a Key set")
				}
				et.Key = ba.Txn.Key
				if len(et.IntentSpans) > 0 {
					// TODO(tschottdorf): it may be useful to allow this later.
					// That would be part of a possible plan to allow txns which
					// write on multiple coordinators.
					return nil, roachpb.NewErrorf("client must not pass intents to EndTransaction")
				}
			}
		}

		if pErr := func() *roachpb.Error {
			tc.mu.Lock()
			defer tc.mu.Unlock()

			// Copy a few fields from the request's txn. This is technically only
			// required during the first send, as these fields are set before
			// the first send and can't change afterwards. Keeping these fields in
			// sync between the TxnCoordSender and the client.Txn is needed because,
			// when the TxnCoordSender synthesizes TransactionAbortedErrors, it
			// creates a new proto that it passes to the client.Txn and then these
			// fields are used when creating that proto that will then be used for the
			// client.Txn. On subsequent retries of the transaction, it's important
			// for the values of these fields to have been preserved because that
			// makes future calls to txn.SetIsolation() and such no-ops.
			// If this makes no sense it's because the TxnCoordSender having a copy of
			// the Transaction proto generally makes no sense.
			tc.mu.meta.Txn.Name = ba.Txn.Name
			tc.mu.meta.Txn.Isolation = ba.Txn.Isolation
			tc.mu.meta.Txn.Priority = ba.Txn.Priority

			if pErr := tc.maybeRejectClientLocked(ctx, ba.Txn.ID); pErr != nil {
				return pErr
			}
			tc.mu.meta.CommandCount += int32(len(ba.Requests))

			if !hasET {
				return nil
			}
			// Everything below is carried out only when trying to finish a txn.
			if tc.typ == client.LeafTxn {
				return roachpb.NewErrorf("cannot commit on a leaf transaction coordinator")
			}

			// Populate et.IntentSpans, taking into account both any existing
			// and new writes, and taking care to perform proper deduplication.
			et.IntentSpans = append([]roachpb.Span(nil), tc.mu.meta.Intents...)
			intentsSizeBytes := tc.mu.intentsSizeBytes
			// Defensively set distinctSpans to false if we had any previous
			// writes in this transaction. This effectively limits the distinct
			// spans optimization to 1pc transactions.
			distinctSpans := len(tc.mu.meta.Intents) == 0

			// We can't pass in a batch response here to better limit the key
			// spans as we don't know what is going to be affected. This will
			// affect queries such as `DELETE FROM my.table LIMIT 10` when
			// executed as a 1PC transaction. e.g.: a (BeginTransaction,
			// DeleteRange, EndTransaction) batch.
			ba.IntentSpanIterate(nil, func(span roachpb.Span) {
				et.IntentSpans = append(et.IntentSpans, span)
				intentsSizeBytes += int64(len(span.Key) + len(span.EndKey))
			})
			var err error
			if et.IntentSpans, intentsSizeBytes, err = tc.maybeCondenseIntentSpans(
				ctx, et.IntentSpans, intentsSizeBytes,
			); err != nil {
				return roachpb.NewError(err)
			}
			// TODO(peter): Populate DistinctSpans on all batches, not just batches
			// which contain an EndTransactionRequest.
			var distinct bool
			et.IntentSpans, distinct = roachpb.MergeSpans(et.IntentSpans)
			ba.Header.DistinctSpans = distinct && distinctSpans
			if len(et.IntentSpans) == 0 {
				// If there aren't any intents, then there's factually no
				// transaction to end. Read-only txns have all of their state
				// in the client.
				return roachpb.NewErrorf("cannot commit a read-only transaction")
			}
			tc.mu.meta.Intents = et.IntentSpans
			tc.mu.intentsSizeBytes = intentsSizeBytes

			if tc.mu.meta.Txn.IsSerializable() && tc.mu.meta.RefreshValid &&
				len(tc.mu.meta.RefreshReads) == 0 && len(tc.mu.meta.RefreshWrites) == 0 {
				et.NoRefreshSpans = true
			}
			return nil
		}(); pErr != nil {
			return nil, pErr
		}

		if hasET && log.V(3) {
			for _, intent := range et.IntentSpans {
				log.Infof(ctx, "intent: [%s,%s)", intent.Key, intent.EndKey)
			}
		}
	}

	// Send the command through wrapped sender, handling retry
	// opportunities in case of error.
	var br *roachpb.BatchResponse
	{
		var pErr *roachpb.Error
		if br, pErr = tc.wrapped.Send(ctx, ba); pErr != nil {
			br, pErr = tc.maybeRetrySend(ctx, &ba, br, pErr)
		}

		if pErr = tc.updateState(ctx, startNS, ba, br, pErr); pErr != nil {
			log.VEventf(ctx, 2, "error: %s", pErr)
			return nil, pErr
		}
	}

	if br.Txn == nil {
		return br, nil
	}

	if _, ok := ba.GetArg(roachpb.EndTransaction); !ok {
		return br, nil
	}
	// If the linearizable flag is set, we want to make sure that all the
	// clocks in the system are past the commit timestamp of the transaction.
	// This is guaranteed if either - the commit timestamp is MaxOffset behind
	// startNS - MaxOffset ns were spent in this function when returning to the
	// client. Below we choose the option that involves less waiting, which is
	// likely the first one unless a transaction commits with an odd timestamp.
	//
	// Can't use linearizable mode with clockless reads since in that case we
	// don't know how long to sleep - could be forever!
	if tsNS := br.Txn.Timestamp.WallTime; startNS > tsNS {
		startNS = tsNS
	}
	maxOffset := tc.clock.MaxOffset()
	sleepNS := maxOffset -
		time.Duration(tc.clock.PhysicalNow()-startNS)

	if maxOffset != timeutil.ClocklessMaxOffset && tc.linearizable && sleepNS > 0 {
		defer func() {
			if log.V(1) {
				log.Infof(ctx, "%v: waiting %s on EndTransaction for linearizability", br.Txn.Short(), duration.Truncate(sleepNS, time.Millisecond))
			}
			time.Sleep(sleepNS)
		}()
	}
	if br.Txn.Status != roachpb.PENDING {
		tc.mu.Lock()
		tc.mu.meta.Txn = br.Txn.Clone()
		_, hasBT := ba.GetArg(roachpb.BeginTransaction)
		onePC := br.Txn.Status == roachpb.COMMITTED && hasBT
		if onePC {
			tc.metrics.Commits1PC.Inc(1)
		}
		tc.cleanupTxnLocked(ctx, done)
		tc.mu.Unlock()
	}
	return br, nil
}

// maybeRetrySend handles two retry cases at the txn coord sender level.
//
// 1) If the batch requires a transaction, it's wrapped in a new
//    transaction and resent.
// 2) If the error is a retry condition which might be retried directly
//    if the spans collected during the transaction can be refreshed,
//    proving that the transaction can be committed at a higher timestamp.
func (tc *TxnCoordSender) maybeRetrySend(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {

	if _, ok := pErr.GetDetail().(*roachpb.OpRequiresTxnError); ok {
		return tc.resendWithTxn(ctx, *ba)
	}

	// With mixed success, we can't attempt a retry without potentially
	// succeeding at the same conditional put or increment request
	// twice; return the wrapped error instead. Because the dist sender
	// splits up batches to send to multiple ranges in parallel, and
	// then combines the results, partial success makes it very
	// difficult to determine what can be retried.
	if aPSErr, ok := pErr.GetDetail().(*roachpb.MixedSuccessError); ok {
		log.VEventf(ctx, 2, "got partial success; cannot retry %s (pErr=%s)", ba, aPSErr.Wrapped)
		return nil, aPSErr.Wrapped
	}

	// Check for an error which can be retried after updating spans.
	//
	// Note that we can only restart on root transactions because those are the
	// only places where we have all of the refresh spans. If this is a leaf, as
	// in a distributed sql flow, we need to propagate the error to the root for
	// an epoch restart.
	canRetry, retryTxn := roachpb.CanTransactionRetryAtRefreshedTimestamp(ctx, pErr)
	if !canRetry || tc.typ == client.LeafTxn ||
		!tc.st.Version.IsMinSupported(cluster.VersionTxnSpanRefresh) {
		return nil, pErr
	}

	// If a prefix of the batch was executed, collect refresh spans for
	// that executed portion, and retry the remainder. The canonical
	// case is a batch split between everything up to but not including
	// the EndTransaction. Requests up to the EndTransaction succeed,
	// but the EndTransaction fails with a retryable error. We want to
	// retry only the EndTransaction.
	ba.UpdateTxn(retryTxn)
	retryBa := *ba
	if br != nil {
		doneBa := *ba
		doneBa.Requests = ba.Requests[:len(br.Responses)]
		log.VEventf(ctx, 2, "collecting refresh spans after partial batch execution of %s", doneBa)
		tc.mu.Lock()
		if !tc.appendRefreshSpansLocked(ctx, doneBa, br) {
			tc.mu.Unlock()
			return nil, pErr
		}
		tc.mu.meta.Txn.RefreshedTimestamp.Forward(retryTxn.RefreshedTimestamp)
		tc.mu.Unlock()
		retryBa.Requests = ba.Requests[len(br.Responses):]
	}

	log.VEventf(ctx, 2, "retrying %s at refreshed timestamp %s because of %s",
		retryBa, retryTxn.RefreshedTimestamp, pErr)

	// Try updating the txn spans so we can retry.
	if ok := tc.tryUpdatingTxnSpans(ctx, retryTxn); !ok {
		return nil, pErr
	}

	// We've refreshed all of the read spans successfully and set
	// newBa.Txn.RefreshedTimestamp to the current timestamp. Submit the
	// batch again.
	retryBr, retryErr := tc.wrapped.Send(ctx, retryBa)
	if retryErr != nil {
		log.VEventf(ctx, 2, "retry failed with %s", retryErr)
		return nil, retryErr
	}
	log.VEventf(ctx, 2, "retry successful @%s", retryBa.Txn.Timestamp)

	// On success, combine responses if applicable and set error to nil.
	if br != nil {
		br.Responses = append(br.Responses, retryBr.Responses...)
		retryBr.CollectedSpans = append(br.CollectedSpans, retryBr.CollectedSpans...)
		br.BatchResponse_Header = retryBr.BatchResponse_Header
	} else {
		br = retryBr
	}

	tc.metrics.AutoRetries.Inc(1)

	return br, nil
}

// appendRefreshSpansLocked appends refresh spans from the supplied batch
// request, qualified by the batch response where appropriate. Returns
// whether the batch transaction's refreshed timestamp is greater or equal
// to the max refreshed timestamp used so far with this sender.
//
// The batch refreshed timestamp and the max refreshed timestamp for
// the sender can get out of step because the txn coord sender can be
// used concurrently (i.e. when using the "RETURNING NOTHING"
// syntax). What we don't want is to append refreshes which are
// already too old compared to the max refreshed timestamp that's already
// in use with this sender. In that case the caller should return an
// error for client-side retry.
func (tc *TxnCoordSender) appendRefreshSpansLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) bool {
	origTS := ba.Txn.OrigTimestamp
	origTS.Forward(ba.Txn.RefreshedTimestamp)
	if origTS.Less(tc.mu.meta.Txn.RefreshedTimestamp) {
		log.VEventf(ctx, 2, "txn orig timestamp %s < sender refreshed timestamp %s",
			origTS, tc.mu.meta.Txn.RefreshedTimestamp)
		return false
	}
	ba.RefreshSpanIterate(br, func(span roachpb.Span, write bool) {
		if log.V(3) {
			log.Infof(ctx, "refresh: %s write=%t", span, write)
		}
		if write {
			tc.mu.meta.RefreshWrites = append(tc.mu.meta.RefreshWrites, span)
		} else {
			tc.mu.meta.RefreshReads = append(tc.mu.meta.RefreshReads, span)
		}
		tc.mu.refreshSpansBytes += int64(len(span.Key) + len(span.EndKey))
	})
	return true
}

// tryUpdatingTxnSpans sends Refresh and RefreshRange commands to all
// spans read during the transaction to ensure that no writes were
// written more recently than the original transaction timestamp. All
// implicated timestamp caches are updated with the final transaction
// timestamp. On success, returns true and an updated BatchRequest
// containing a transaction whose original timestamp and timestamp
// have been set to the same value.
func (tc *TxnCoordSender) tryUpdatingTxnSpans(
	ctx context.Context, refreshTxn *roachpb.Transaction,
) bool {
	tc.mu.Lock()
	refreshReads := tc.mu.meta.RefreshReads
	refreshWrites := tc.mu.meta.RefreshWrites
	refreshValid := tc.mu.meta.RefreshValid
	tc.mu.Unlock()

	if !refreshValid {
		log.VEvent(ctx, 2, "can't refresh txn spans; not valid")
		return false
	} else if len(refreshReads) == 0 && len(refreshWrites) == 0 {
		log.VEvent(ctx, 2, "there are no txn spans to refresh")
		return true
	}

	// Refresh all spans (merge first).
	refreshSpanBa := roachpb.BatchRequest{}
	refreshSpanBa.Txn = refreshTxn
	addRefreshes := func(refreshes []roachpb.Span, write bool) {
		for _, u := range refreshes {
			var req roachpb.Request
			if len(u.EndKey) == 0 {
				req = &roachpb.RefreshRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(u),
					Write:         write,
				}
			} else {
				req = &roachpb.RefreshRangeRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(u),
					Write:         write,
				}
			}
			refreshSpanBa.Add(req)
			log.VEventf(ctx, 2, "updating span %s @%s - @%s to avoid serializable restart",
				req.Header().Span(), refreshTxn.OrigTimestamp, refreshTxn.Timestamp)
		}
	}
	addRefreshes(refreshReads, false)
	addRefreshes(refreshWrites, true)
	if _, batchErr := tc.wrapped.Send(ctx, refreshSpanBa); batchErr != nil {
		log.VEventf(ctx, 2, "failed to refresh txn spans (%s); propagating original retry error", batchErr)
		return false
	}

	return true
}

func (tc *TxnCoordSender) appendAndCondenseIntentsLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) {
	ba.IntentSpanIterate(br, func(span roachpb.Span) {
		tc.mu.meta.Intents = append(tc.mu.meta.Intents, span)
		tc.mu.intentsSizeBytes += int64(len(span.Key) + len(span.EndKey))
	})
	if condensedIntents, condensedIntentsSize, err :=
		tc.maybeCondenseIntentSpans(ctx, tc.mu.meta.Intents, tc.mu.intentsSizeBytes); err != nil {
		log.VEventf(ctx, 2, "failed to condense intent spans (%s); skipping", err)
	} else {
		tc.mu.meta.Intents, tc.mu.intentsSizeBytes = condensedIntents, condensedIntentsSize
	}
}

type spanBucket struct {
	rangeID roachpb.RangeID
	size    int64
	spans   []roachpb.Span
}

// maybeCondenseIntentSpans avoids sending massive EndTransaction
// requests which can consume excessive memory at evaluation time and
// in the txn coordinator sender itself. Spans are condensed based on
// current range boundaries. Returns the condensed set of spans and
// the new total spans size. Note that errors can be returned if the
// range iterator fails.
func (tc *TxnCoordSender) maybeCondenseIntentSpans(
	ctx context.Context, spans []roachpb.Span, spansSize int64,
) ([]roachpb.Span, int64, error) {
	if spansSize < maxTxnIntentsBytes.Get(&tc.st.SV) {
		return spans, spansSize, nil
	}
	// Only condense if the wrapped sender is a distributed sender.
	ds, ok := tc.wrapped.(*DistSender)
	if !ok {
		return spans, spansSize, nil
	}
	// Sort the spans by start key.
	sort.Slice(spans, func(i, j int) bool { return spans[i].Key.Compare(spans[j].Key) < 0 })

	// Divide them by range boundaries and condense. Iterate over spans
	// using a range iterator and add each to a bucket keyed by range
	// ID. Local keys are kept in a new slice and not added to buckets.
	buckets := []*spanBucket{}
	localSpans := []roachpb.Span{}
	ri := NewRangeIterator(ds)
	for _, s := range spans {
		if keys.IsLocal(s.Key) {
			localSpans = append(localSpans, s)
			continue
		}
		ri.Seek(ctx, roachpb.RKey(s.Key), Ascending)
		if !ri.Valid() {
			return nil, 0, ri.Error().GoError()
		}
		rangeID := ri.Desc().RangeID
		if l := len(buckets); l > 0 && buckets[l-1].rangeID == rangeID {
			buckets[l-1].spans = append(buckets[l-1].spans, s)
		} else {
			buckets = append(buckets, &spanBucket{rangeID: rangeID, spans: []roachpb.Span{s}})
		}
		buckets[len(buckets)-1].size += int64(len(s.Key) + len(s.EndKey))
	}

	// Sort the buckets by size and collapse from largest to smallest
	// until total size of uncondensed spans no longer exceeds threshold.
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].size > buckets[j].size })
	spans = localSpans // reset to hold just the local spans; will add newly condensed and remainder
	for _, bucket := range buckets {
		// Condense until we get to half the threshold.
		if spansSize <= maxTxnIntentsBytes.Get(&tc.st.SV)/2 {
			// Collect remaining spans from each bucket into uncondensed slice.
			spans = append(spans, bucket.spans...)
			continue
		}
		spansSize -= bucket.size
		// TODO(spencer): consider further optimizations here to create
		// more than one span out of a bucket to avoid overly broad span
		// combinations.
		cs := bucket.spans[0]
		for _, s := range bucket.spans[1:] {
			cs = cs.Combine(s)
			if !cs.Valid() {
				return nil, 0, errors.Errorf("combining span %s yielded invalid result", s)
			}
		}
		spansSize += int64(len(cs.Key) + len(cs.EndKey))
		spans = append(spans, cs)
	}

	return spans, spansSize, nil
}

// maybeRejectClientLocked checks whether the (transactional) request is in a
// state that prevents it from continuing, such as the coordinator having
// considered the client abandoned, or a heartbeat having reported an error.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, txnID uuid.UUID,
) *roachpb.Error {
	// Check whether the transaction is still tracked and has a chance of
	// completing. It's possible that the coordinator learns about the
	// transaction having terminated from a heartbeat, and GC queue correctness
	// (along with common sense) mandates that we don't let the client
	// continue.
	switch {
	case tc.mu.state == aborted:
		fallthrough
	case tc.mu.meta.Txn.Status == roachpb.ABORTED:
		abortedErr := roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), &tc.mu.meta.Txn)
		// TODO(andrei): figure out a UserPriority to use here.
		newTxn := roachpb.PrepareTransactionForRetry(
			ctx, abortedErr,
			// priority is not used for aborted errors
			roachpb.NormalUserPriority,
			tc.clock)
		return roachpb.NewError(roachpb.NewHandledRetryableTxnError(
			abortedErr.Message, txnID, newTxn))

	case tc.mu.meta.Txn.Status == roachpb.COMMITTED:
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"transaction is already committed"), &tc.mu.meta.Txn)

	default:
		return nil
	}
}

// validateTxn validates properties of a txn specified on a request.
// The transaction is expected to be initialized by the time it reaches
// the TxnCoordSender.
func (tc *TxnCoordSender) validateTxnForBatch(ctx context.Context, ba *roachpb.BatchRequest) error {
	if len(ba.Requests) == 0 {
		return errors.Errorf("empty batch with txn")
	}
	ba.Txn.AssertInitialized(ctx)

	var haveBeginTxn bool
	for _, req := range ba.Requests {
		args := req.GetInner()
		if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
			if haveBeginTxn {
				return errors.Errorf("begin transaction requested twice in the same batch: %s", ba.Txn)
			}
			if ba.Txn.Key == nil {
				return errors.Errorf("transaction with BeginTxnRequest missing anchor key: %v", ba)
			}
			haveBeginTxn = true
		}
	}
	return nil
}

// cleanupTxnLocked is called when a transaction ends. The heartbeat
// goroutine is signaled to stop. The TxnCoordSender's state is set to `state`.
// Future Send() calls are rejected.
func (tc *TxnCoordSender) cleanupTxnLocked(ctx context.Context, state txnCoordState) {
	tc.mu.state = state
	if tc.mu.onFinishFn != nil {
		// rejectErr is guaranteed to be non-nil because state is done or
		// aborted on cleanup.
		rejectErr := tc.maybeRejectClientLocked(ctx, tc.mu.meta.Txn.ID).GetDetail()
		if rejectErr == nil {
			log.Fatalf(ctx, "expected non-nil rejectErr on txn coord state %v", state)
		}
		tc.mu.onFinishFn(rejectErr)
	}
	tc.mu.meta.Intents = nil
	tc.mu.intentsSizeBytes = 0

	// The heartbeat might've already removed the record. Or we may have already
	// closed txnEnd but we are racing with the heartbeat cleanup.
	if tc.mu.txnEnd == nil {
		return
	}
	// Trigger heartbeat shutdown.
	log.VEvent(ctx, 2, "coordinator stops")
	close(tc.mu.txnEnd)
	tc.mu.txnEnd = nil
}

// finalTxnStatsLocked collects a transaction's final statistics. Returns
// the duration, restarts, and finalized txn status.
func (tc *TxnCoordSender) finalTxnStatsLocked() (duration, restarts int64, status roachpb.TransactionStatus) {
	duration = tc.clock.PhysicalNow() - tc.mu.firstUpdateNanos
	restarts = int64(tc.mu.meta.Txn.Epoch)
	status = tc.mu.meta.Txn.Status
	return duration, restarts, status
}

// heartbeatLoop periodically sends a HeartbeatTxn RPC to an extant transaction,
// stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents. When the heartbeat stops, the transaction
// stats are updated based on its final disposition.
//
// TODO(wiz): Update (*DBServer).Batch to not use context.TODO().
func (tc *TxnCoordSender) heartbeatLoop(ctx context.Context) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(tc.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	// TODO(tschottdorf): this should join to the trace of the request
	// which starts this goroutine.
	sp := tc.AmbientContext.Tracer.StartSpan(opHeartbeatLoop)
	defer sp.Finish()
	ctx = opentracing.ContextWithSpan(ctx, sp)

	defer func() {
		tc.mu.Lock()
		if tc.mu.txnEnd != nil {
			tc.mu.txnEnd = nil
		}
		duration, restarts, status := tc.finalTxnStatsLocked()
		tc.mu.hbRunning = false
		tc.mu.Unlock()
		tc.updateStats(duration, restarts, status)
	}()

	var closer <-chan struct{}
	{
		tc.mu.Lock()
		closer = tc.mu.txnEnd
		tc.mu.Unlock()
		if closer == nil {
			return
		}
	}
	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !tc.heartbeat(ctx) {
				return
			}
		case <-closer:
			// Transaction finished normally.
			return
		case <-tc.stopper.ShouldQuiesce():
			return
		}
	}
}

// abortTxnAsync sends an EndTransaction asynchronously to the wrapped Sender.
func abortTxnAsync(
	ctx context.Context,
	txn roachpb.Transaction,
	intentSpans []roachpb.Span,
	wrapped client.Sender,
	stopper *stop.Stopper,
) {
	log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := stopper.RunAsyncTask(
		ctx, "kv.TxnCoordSender: aborting txn", func(ctx context.Context) {
			_, pErr := client.SendWrappedWith(
				ctx, wrapped, roachpb.Header{Txn: &txn}, &roachpb.EndTransactionRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: txn.Key,
					},
					Commit:      false,
					IntentSpans: intentSpans,
					// Resolved intents should maintain an abort span entry to
					// prevent concurrent requests from failing to notice the
					// transaction was aborted.
					Poison: true,
				},
			)
			if pErr != nil {
				log.VErrEventf(ctx, 1,
					"abort due to inactivity failed for %s: %s ", txn, pErr)
			}
		},
	); err != nil {
		log.Warning(ctx, err)
	}
}

// heartbeat sends a HeartbeatTxnRequest to the txn record.
// Errors that carry update txn information (e.g.  TransactionAbortedError) will
// update the txn. Other errors are swallowed.
// Returns true if heartbeating should continue, false if the transaction is no
// longer Pending and so there's no point in heartbeating further.
func (tc *TxnCoordSender) heartbeat(ctx context.Context) bool {
	tc.mu.Lock()
	txn := tc.mu.meta.Txn.Clone()
	tc.mu.Unlock()

	if txn.Status != roachpb.PENDING {
		// A previous iteration has already determined that the transaction is
		// already finalized.
		return false
	}

	if txn.Key == nil {
		return true
	}

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	hb := &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: tc.clock.Now(),
	}
	ba.Add(hb)

	log.VEvent(ctx, 2, "heartbeat")
	br, pErr := tc.wrapped.Send(ctx, ba)

	if pErr != nil {
		log.VEventf(ctx, 2, "heartbeat failed: %s", pErr)

		// If the heartbeat request arrived to find a missing transaction record
		// then we ignore the error. This is possible if the heartbeat loop was
		// started before a BeginTxn request succeeds because of ambiguity in the
		// first write request's response.
		if tse, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); ok &&
			tse.Reason == roachpb.TransactionStatusError_REASON_TXN_NOT_FOUND {
			return true
		}

		// If the error contains updated txn info, ingest it. For example, we might
		// find out this way that the transaction has been Aborted in the meantime
		// (e.g. someone else pushed it).
		if errTxn := pErr.GetTxn(); errTxn != nil {
			tc.mu.Lock()
			tc.mu.meta.Txn.Update(errTxn)
			tc.mu.Unlock()
		}

		return tc.mu.meta.Txn.Status == roachpb.PENDING
	}
	txn.Update(br.Responses[0].GetInner().(*roachpb.HeartbeatTxnResponse).Txn)

	// Give the news to the txn in the txns map. This will update long-running
	// transactions (which may find out that they have to restart in that way),
	// but in particular makes sure that they notice when they've been aborted
	// (in which case we'll give them an error on their next request).
	tc.mu.Lock()
	tc.mu.meta.Txn.Update(&txn)
	tc.mu.Unlock()

	return true
}

// startHeartbeatLoop starts a heartbeat loop in a different goroutine.
func (tc *TxnCoordSender) startHeartbeatLoop(ctx context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.mu.hbRunning = true
	tc.mu.firstUpdateNanos = tc.clock.PhysicalNow()

	// Only heartbeat the txn record if we're the root transaction.
	if tc.typ != client.RootTxn {
		return nil
	}

	log.VEventf(ctx, 2, "coordinator spawns heartbeat loop")
	// Create a channel to stop the heartbeat with the lock held
	// to avoid a race between the async task and a subsequent commit.
	tc.mu.txnEnd = make(chan struct{})
	// Create a new context so that the heartbeat loop doesn't inherit the
	// caller's cancelation.
	hbCtx := tc.AnnotateCtx(context.Background())
	if err := tc.stopper.RunAsyncTask(
		ctx, "kv.TxnCoordSender: heartbeat loop", func(ctx context.Context) {
			tc.heartbeatLoop(hbCtx)
		}); err != nil {
		// The system is already draining and we can't start the
		// heartbeat. We refuse new transactions for now because
		// they're likely not going to have all intents committed.
		// In principle, we can relax this as needed though.
		tc.cleanupTxnLocked(ctx, aborted)
		duration, restarts, status := tc.finalTxnStatsLocked()
		tc.updateStats(duration, restarts, status)
		return err
	}
	return nil
}

// IsTracking returns true if the heartbeat loop is running.
func (tc *TxnCoordSender) IsTracking() bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return tc.mu.hbRunning
}

// updateState updates the transaction state in both the success and
// error cases, applying those updates to the corresponding txnMeta
// object when adequate. It also updates retryable errors with the
// updated transaction for use by client restarts.
//
// startNS is the time when the request that's updating the state has
// been sent.  This is not used if the request is known to not be the
// one in charge of starting tracking the transaction - i.e. this is
// the case for DistSQL, which just does reads and passes 0.
func (tc *TxnCoordSender) updateState(
	ctx context.Context,
	startNS int64,
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) *roachpb.Error {

	if ba.Txn == nil {
		// Not a transactional request.
		return pErr
	}

	// Iterate over and aggregate refresh spans in the requests,
	// qualified by possible resume spans in the responses, if the txn
	// has serializable isolation and we haven't yet exceeded the max
	// read key bytes.
	tc.mu.Lock()
	if pErr == nil && ba.Txn.IsSerializable() {
		if tc.mu.meta.RefreshValid {
			if !tc.appendRefreshSpansLocked(ctx, ba, br) {
				// The refresh spans are out of date, return a generic client-side retry error.
				pErr = roachpb.NewErrorWithTxn(roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE), br.Txn)
			}
		}
		// Verify and enforce the size in bytes of all read-only spans
		// doesn't exceed the max threshold.
		if tc.mu.refreshSpansBytes > maxTxnRefreshSpansBytes.Get(&tc.st.SV) {
			log.VEventf(ctx, 2, "refresh spans max size exceeded; clearing")
			tc.mu.meta.RefreshReads = nil
			tc.mu.meta.RefreshWrites = nil
			tc.mu.meta.RefreshValid = false
			tc.mu.refreshSpansBytes = 0
		}
	}
	// If the transaction will retry and the refresh spans are
	// exhausted, return a non-retryable error indicating that the
	// transaction is too large and should potentially be split.
	// We do this to avoid endlessly retrying a txn likely refail.
	if pErr == nil && !tc.mu.meta.RefreshValid &&
		(br.Txn.WriteTooOld || br.Txn.OrigTimestamp != br.Txn.Timestamp) {
		pErr = roachpb.NewErrorWithTxn(
			errors.New("transaction is too large to complete; try splitting into pieces"), br.Txn,
		)
	}
	tc.mu.Unlock()

	txnID := ba.Txn.ID
	var newTxn roachpb.Transaction
	if pErr == nil {
		newTxn.Update(ba.Txn)
		newTxn.Update(br.Txn)
	} else {
		// Only handle transaction retry errors if this is a root transaction.
		if pErr.TransactionRestart != roachpb.TransactionRestart_NONE &&
			tc.typ == client.RootTxn {
			errTxnID := pErr.GetTxn().ID // The ID of the txn that needs to be restarted.
			if errTxnID != txnID {
				// KV should not return errors for transactions other than the one in
				// the BatchRequest.
				log.Fatalf(ctx, "retryable error for the wrong txn. ba.Txn: %s. pErr: %s",
					ba.Txn, pErr)
			}
			// If the error is a transaction retry error, update metrics to
			// reflect the reason for the restart.
			// TODO(spencer): this code path does not account for retry errors
			//   experienced by dist sql (see internal/client/txn.go).
			if tErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); ok {
				switch tErr.Reason {
				case roachpb.RETRY_WRITE_TOO_OLD:
					tc.metrics.RestartsWriteTooOld.Inc(1)
				case roachpb.RETRY_DELETE_RANGE:
					tc.metrics.RestartsDeleteRange.Inc(1)
				case roachpb.RETRY_SERIALIZABLE:
					tc.metrics.RestartsSerializable.Inc(1)
				case roachpb.RETRY_POSSIBLE_REPLAY:
					tc.metrics.RestartsPossibleReplay.Inc(1)
				}
			}
			newTxn = roachpb.PrepareTransactionForRetry(ctx, pErr, ba.UserPriority, tc.clock)

			// We'll pass a HandledRetryableTxnError up to the next layer.
			pErr = roachpb.NewError(
				roachpb.NewHandledRetryableTxnError(
					pErr.Message,
					errTxnID, // the id of the transaction that encountered the error
					newTxn))

			// If the ID changed, it means we had to start a new transaction and the
			// old one is toast. This TxnCoordSender cannot be used any more - future
			// Send() calls will be rejected; the client is supposed to create a new
			// one.
			if errTxnID != newTxn.ID {
				tc.mu.Lock()
				tc.cleanupTxnLocked(ctx, aborted)

				// NB: We use context.Background() here because we don't want a canceled
				// context to interrupt the aborting.
				abortCtx := tc.AnnotateCtx(context.Background())
				// Clone the intents and the txn to avoid data races.
				intentSpans, _ := roachpb.MergeSpans(append([]roachpb.Span(nil), tc.mu.meta.Intents...))
				txn := tc.mu.meta.Txn.Clone()

				abortTxnAsync(abortCtx, txn, intentSpans, tc.wrapped, tc.stopper)
				tc.mu.Unlock()
				return pErr
			}
			// Reset state as this is a retryable txn error. Note that intents are
			// tracked cumulatively across epochs on retries.
			log.VEventf(ctx, 2, "resetting epoch-based coordinator state on retry")
			tc.mu.Lock()
			tc.mu.meta.CommandCount = 0
			tc.mu.meta.RefreshReads = nil
			tc.mu.meta.RefreshWrites = nil
			tc.mu.meta.RefreshValid = true
			tc.mu.refreshSpansBytes = 0
			tc.mu.Unlock()
		} else {
			// We got a non-retryable error, or a retryable error at a leaf
			// transaction, and need to pass responsibility for handling it
			// up to the root transaction.

			newTxn.Update(ba.Txn)
			if errTxn := pErr.GetTxn(); errTxn != nil {
				newTxn.Update(errTxn)
			}

			// Update the txn in the error to reflect the TxnCoordSender's state.
			//
			// Avoid changing existing errors because sometimes they escape into
			// goroutines and data races can occur.
			pErrShallow := *pErr
			pErrShallow.SetTxn(&newTxn) // SetTxn clones newTxn
			pErr = &pErrShallow
		}
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	// For successful transactional requests, keep the written intents and
	// the updated transaction record to be sent along with the reply.
	// The transaction metadata is created with the first writing operation.
	//
	// For serializable requests, keep the read spans in the event we get
	// a serializable retry error. We can use the set of read spans to
	// avoid retrying the transaction if all the spans can be updated to
	// the current transaction timestamp.
	//
	// Adding the intents even on error reduces the likelihood of dangling
	// intents blocking concurrent writers for extended periods of time.
	// See #3346.
	tc.appendAndCondenseIntentsLocked(ctx, ba, br)

	// Update our record of this transaction, even on error.
	tc.mu.meta.Txn.Update(&newTxn)
	tc.mu.lastUpdateNanos = tc.clock.PhysicalNow()

	if pErr != nil {
		// On rollback error, stop the heartbeat loop. No more requests can come
		// after a rollback, and there's nobody else to stop the heartbeat loop.
		// The rollback success, like the commit success, is handled similarly
		// below.
		et, isEnding := ba.GetArg(roachpb.EndTransaction)
		if isEnding && !et.(*roachpb.EndTransactionRequest).Commit {
			tc.cleanupTxnLocked(ctx, aborted)
		}
	}

	return pErr
}

// TODO(tschottdorf): this method is somewhat awkward but unless we want to
// give this error back to the client, our options are limited. We'll have to
// run the whole thing for them, or any restart will still end up at the client
// which will not be prepared to be handed a Txn.
func (tc *TxnCoordSender) resendWithTxn(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Run a one-off transaction with that single command.
	if log.V(1) {
		log.Infof(ctx, "%s: auto-wrapping in txn and re-executing: ", ba)
	}
	// TODO(bdarnell): need to be able to pass other parts of DBContext
	// through here.
	dbCtx := client.DefaultDBContext()
	dbCtx.UserPriority = ba.UserPriority
	dbCtx.Stopper = tc.stopper
	tmpDB := client.NewDBWithContext(tc.AmbientContext, tc.TxnCoordSenderFactory, tc.clock, dbCtx)
	var br *roachpb.BatchResponse
	err := tmpDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetDebugName("auto-wrap")
		b := txn.NewBatch()
		b.Header = ba.Header
		for _, arg := range ba.Requests {
			req := arg.GetInner().ShallowCopy()
			b.AddRawRequest(req)
		}
		err := txn.CommitInBatch(ctx, b)
		br = b.RawResponse()
		return err
	})
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br.Txn = nil // hide the evidence
	return br, nil
}

// updateStats updates transaction metrics after a transaction finishes.
func (tc *TxnCoordSender) updateStats(duration, restarts int64, status roachpb.TransactionStatus) {
	tc.metrics.Durations.RecordValue(duration)
	tc.metrics.Restarts.RecordValue(restarts)
	switch status {
	case roachpb.ABORTED:
		tc.metrics.Aborts.Inc(1)
	case roachpb.PENDING:
		// NOTE(andrei): Getting a PENDING status here is possible if the heartbeat
		// loop has stopped because the stopper is quiescing.
	case roachpb.COMMITTED:
		tc.metrics.Commits.Inc(1)
	}
}
