package org.infinispan.conflict.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.conflict.GetBucketEntriesCommand;
import org.infinispan.commands.conflict.GetBucketHashesCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.EntryMergePolicyFactoryRegistry;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import jakarta.transaction.TransactionManager;

/**
 * Default implementation of {@link InternalConflictManager} that identifies and resolves entries
 * that diverged across replicas after a network partition heals.
 *
 * <h2>Merkle-tree conflict resolution</h2>
 *
 * <p>Rather than fetching <em>all</em> entries from <em>all</em> write owners for <em>every</em>
 * segment (which scales linearly with data size regardless of actual conflict count), this
 * implementation uses a three-level hash comparison hierarchy that progressively narrows the
 * scope of data transfer. When conflicts are sparse (the common case), this dramatically reduces
 * network traffic.</p>
 *
 * <pre>
 * Level 1: Segment Hash Comparison
 *   All hashes match? ─► Skip segment entirely (zero entry transfer)
 *          │
 *          ▼ mismatch
 * Level 2: Bucket Hash Comparison
 *   Compare 32 buckets per segment
 *   Identify mismatched bucket IDs
 *          │
 *          ▼ mismatched buckets
 * Level 3: Selective Entry Fetch
 *   Fetch entries only from mismatched buckets (~3% per bucket)
 *          │
 *          ▼ fallback on any error
 * Level 0: Full Segment Fetch (original behavior)
 *   getAllReplicasForSegment() — fetch everything
 * </pre>
 *
 * <h2>Hashing scheme</h2>
 *
 * <p><strong>Entry hashing.</strong> Each cache entry is hashed by marshalling its key and value
 * to byte arrays and computing MurmurHash3_x64_64 on each:</p>
 * <pre>
 * entryHash(entry) = MurmurHash3(keyBytes, 9001) XOR MurmurHash3(valueBytes, 9001)
 * </pre>
 *
 * <p><strong>Segment hashing.</strong> A segment hash is the XOR of all entry hashes within that
 * segment:</p>
 * <pre>
 * segmentHash(seg) = entryHash(e1) XOR entryHash(e2) XOR ... XOR entryHash(eN)
 * </pre>
 * <p>XOR is commutative and associative, so the result is independent of iteration order.</p>
 *
 * <p><strong>Bucket hashing.</strong> Each segment is subdivided into 32 buckets. An entry's
 * bucket is determined solely by its key:</p>
 * <pre>
 * bucketId(key) = MurmurHash3(keyBytes, 9001) AND 0x1F    (bitmask for 32 buckets)
 * </pre>
 * <p>Each bucket hash is the XOR of entry hashes belonging to that bucket. Because XOR is
 * associative, the segment hash can be derived from bucket hashes without a separate iteration:</p>
 * <pre>
 * segmentHash(seg) = bucketHash(seg, 0) XOR bucketHash(seg, 1) XOR ... XOR bucketHash(seg, 31)
 * entryCount(seg)  = count(seg, 0) + count(seg, 1) + ... + count(seg, 31)
 * </pre>
 *
 * <h2>Data structures</h2>
 *
 * <ul>
 *   <li>{@link SegmentHash} — holds {@code segmentId}, {@code hash} (long), and
 *       {@code entryCount} (int). Two segment hashes "match" when both hash and count are equal.</li>
 *   <li>{@link BucketHash} — holds {@code segmentId}, {@code bucketId}, {@code hash} (long),
 *       and {@code entryCount} (int). Same matching semantics as {@link SegmentHash}.</li>
 * </ul>
 *
 * <h2>RPC commands</h2>
 *
 * <ul>
 *   <li>{@link GetBucketHashesCommand} — sent once per remote node covering all segments that
 *       node owns. Returns a flat list of {@link BucketHash} records grouped by segment.
 *       This amortizes RPC latency across all segments.</li>
 *   <li>{@link GetBucketEntriesCommand} — sent per segment to all remote write owners.
 *       Requests entries only from specified bucket IDs. When {@code bucketIds.size() >= bucketCount},
 *       the {@code bucketForKey()} computation is skipped and all entries are returned.</li>
 * </ul>
 *
 * <h2>Flow</h2>
 *
 * <p><strong>Phase 1: Prefetch bucket hashes.</strong> When conflict resolution starts, the
 * {@code ReplicaSpliterator} constructor calls {@code prefetchAllBucketHashes()}, sending one
 * batched RPC per remote node covering all owned segments, and computing local bucket hashes
 * in bulk.</p>
 *
 * <p><strong>Phase 2: Per-segment comparison.</strong> For each segment,
 * {@code findMismatchedBuckets()} performs a purely local comparison using the prefetched data
 * (no RPCs). Segment hashes are derived from bucket hashes. If all replicas match, the segment
 * is skipped entirely. For small segments (≤ {@value #SMALL_SEGMENT_THRESHOLD} entries),
 * per-bucket narrowing is skipped and all entries are fetched directly.</p>
 *
 * <p><strong>Phase 3: Selective entry fetch.</strong> Only entries from mismatched buckets are
 * fetched via {@code getReplicasForBuckets()}. The result is formatted identically to
 * {@code getAllReplicasForSegment()} output and fed into the existing
 * {@code filterConsistentEntries} + merge policy pipeline.</p>
 *
 * <p><strong>Fallback.</strong> If any step fails (RPC error, unexpected response, prefetch
 * failure), the system falls back to the original {@code getAllReplicasForSegment()} full fetch.
 * The optimization is fail-safe — it can only help, never break correctness.</p>
 *
 * <h2>Optimizations</h2>
 *
 * <ol>
 *   <li><strong>Single-pass dual hash.</strong> Bucket hashes are computed in a single iteration
 *       over the data container. The segment hash is derived from bucket hashes via XOR
 *       associativity, avoiding a second pass.</li>
 *   <li><strong>Batched RPCs.</strong> Instead of one RPC per segment per node, a single
 *       {@link GetBucketHashesCommand} is sent per remote node covering all segments. This
 *       reduces RPC count from {@code segments × remoteNodes} to {@code remoteNodes} (typically
 *       1–2).</li>
 *   <li><strong>Small segment threshold.</strong> Segments with ≤ {@value #SMALL_SEGMENT_THRESHOLD}
 *       entries skip per-bucket narrowing entirely. When a segment is small, the cost of key
 *       marshalling for {@code bucketForKey()} exceeds the savings from transferring fewer
 *       entries.</li>
 *   <li><strong>All-buckets fast path.</strong> When all 32 buckets are mismatched (or the small
 *       segment threshold triggers), {@link GetBucketEntriesCommand} and
 *       {@code getReplicasForBuckets()} detect that {@code bucketIds.size() >= bucketCount} and
 *       skip the {@code bucketForKey()} computation, avoiding unnecessary key marshalling.</li>
 * </ol>
 *
 * <h2>Cost analysis</h2>
 *
 * <table>
 *   <tr><th>Scenario</th><th>Before</th><th>After</th></tr>
 *   <tr><td>No conflicts (common case)</td>
 *       <td>Fetch all entries from all segments</td>
 *       <td>1 batched RPC per remote node (hashes only), zero entry transfer</td></tr>
 *   <tr><td>1 conflicting key in 1 segment (10K entries, 256 segments)</td>
 *       <td>10K entries transferred</td>
 *       <td>~312 entries transferred (1/32 of segment)</td></tr>
 *   <tr><td>All entries conflicting</td>
 *       <td>Full fetch</td>
 *       <td>Full fetch (same as before, plus small hash overhead)</td></tr>
 *   <tr><td>Segment with ≤ 64 entries</td>
 *       <td>Full fetch</td>
 *       <td>Direct entry fetch (skips bucket narrowing overhead)</td></tr>
 * </table>
 *
 * <p>The hash comparison overhead is minimal: 32 {@link BucketHash} records (32 × 20 bytes =
 * 640 bytes) per segment, compared to potentially thousands of serialized cache entries per
 * segment in the full-fetch path.</p>
 *
 * @author Ryan Emerson
 * @see SegmentHash
 * @see BucketHash
 * @see SegmentHasher
 * @see GetBucketHashesCommand
 * @see GetBucketEntriesCommand
 */
@Scope(Scopes.NAMED_CACHE)
public class DefaultConflictManager<K, V> implements InternalConflictManager<K, V> {

   private static final Log log = LogFactory.getLog(DefaultConflictManager.class);

   private static final int BUCKET_COUNT = SegmentHasher.DEFAULT_BUCKET_COUNT;
   private static final int SMALL_SEGMENT_THRESHOLD = 64;
   private static final int SEGMENT_CONCURRENCY = 4;
   private static final IntSet ALL_BUCKETS = IntSets.immutableRangeSet(BUCKET_COUNT);
   private static final long localFlags = FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.SKIP_LOCKING;
   private static final long userMergeFlags = FlagBitSets.IGNORE_RETURN_VALUES;
   private static final long autoMergeFlags = FlagBitSets.IGNORE_RETURN_VALUES | FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.SKIP_REMOTE_LOOKUP;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   String cacheName;
   @Inject
   ComponentRef<AsyncInterceptorChain> interceptorChain;
   @Inject
   InvocationHelper invocationHelper;
   @Inject
   Configuration cacheConfiguration;
   @Inject
   CommandsFactory commandsFactory;
   @Inject
   DistributionManager distributionManager;
   @Inject
   InvocationContextFactory invocationContextFactory;
   @Inject
   RpcManager rpcManager;
   @Inject
   ComponentRef<StateConsumer> stateConsumer;
   @Inject
   StateReceiver<K, V> stateReceiver;
   @Inject
   EntryMergePolicyFactoryRegistry mergePolicyRegistry;
   @Inject
   TimeService timeService;
   @Inject
   InternalEntryFactory internalEntryFactory;
   @Inject
   TransactionManager transactionManager;
   @Inject
   KeyPartitioner keyPartitioner;
   @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   @Inject
   Marshaller internalMarshaller;
   @Inject
   InternalDataContainer<K, V> dataContainer;

   private Address localAddress;
   private long conflictTimeout;
   private EntryMergePolicy<K, V> entryMergePolicy;
   private final AtomicBoolean streamInProgress = new AtomicBoolean();
   private final Map<K, VersionRequest> versionRequestMap = new HashMap<>();
   private final Queue<VersionRequest> retryQueue = new ConcurrentLinkedQueue<>();
   private volatile boolean running = false;
   private volatile CompletableFuture<Void> conflictFuture;

   @Start
   public void start() {
      this.localAddress = rpcManager.getAddress();

      PartitionHandlingConfiguration config = cacheConfiguration.clustering().partitionHandling();
      this.entryMergePolicy = mergePolicyRegistry.createInstance(config);

      // TODO make this an explicit configuration param in PartitionHandlingConfiguration
      this.conflictTimeout = cacheConfiguration.clustering().stateTransfer().timeout();

      this.running = true;
      if (log.isTraceEnabled())
         log.tracef("Cache %s starting %s. isRunning=%s", cacheName, getClass().getSimpleName(), !running);
   }

   @Stop
   public void stop() {
      this.running = false;
      synchronized (versionRequestMap) {
         if (log.isTraceEnabled())
            log.tracef("Cache %s stopping %s. isRunning=%s", getClass().getSimpleName(), cacheName, running);
         cancelVersionRequests();
         versionRequestMap.clear();
      }

      streamInProgress.set(false);
   }

   @Override
   public StateReceiver getStateReceiver() {
      return stateReceiver;
   }

   @Override
   public void cancelVersionRequests() {
      if (!running)
         return;

      synchronized (versionRequestMap) {
         versionRequestMap.values().forEach(VersionRequest::cancelRequestIfOutdated);
      }
   }

   @Override
   public void restartVersionRequests() {
      if (!running)
         return;

      VersionRequest request;
      while ((request = retryQueue.poll()) != null) {
         if (log.isTraceEnabled()) log.tracef("Retrying %s", request);
         request.start();
      }
   }

   @Override
   public Map<Address, InternalCacheValue<V>> getAllVersions(final K key) {
      checkIsRunning();

      final VersionRequest request;
      synchronized (versionRequestMap) {
         request = versionRequestMap.computeIfAbsent(key, k -> new VersionRequest(k, stateConsumer.running().isStateTransferInProgress()));
      }

      try {
         return request.completableFuture.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         if (e.getCause() instanceof CacheException)
            throw (CacheException) e.getCause();

         throw new CacheException(e.getCause());
      } finally {
         synchronized (versionRequestMap) {
            versionRequestMap.remove(key);
         }
      }
   }

   @Override
   public Stream<Map<Address, CacheEntry<K, V>>> getConflicts() {
      checkIsRunning();
      return Flowable.fromPublisher(getConflictsPublisher()).blockingStream();
   }

   @Override
   public Publisher<Map<Address, CacheEntry<K, V>>> getConflictsPublisher() {
      checkIsRunning();
      return getConflictsFlowable(distributionManager.getCacheTopology());
   }

   private Flowable<Map<Address, CacheEntry<K, V>>> getConflictsFlowable(LocalizedCacheTopology topology) {
      if (log.isTraceEnabled())
         log.tracef("getConflicts isStateTransferInProgress=%s, topology=%s", stateConsumer.running().isStateTransferInProgress(), topology);
      if (topology.getPhase() != CacheTopology.Phase.CONFLICT_RESOLUTION && stateConsumer.running().isStateTransferInProgress()) {
         throw CLUSTER.getConflictsStateTransferInProgress(cacheName);
      }

      if (!streamInProgress.compareAndSet(false, true))
         throw CLUSTER.getConflictsAlreadyInProgress();

      if (!running) {
         streamInProgress.set(false);
         return Flowable.empty();
      }

      int totalSegments = topology.getWriteConsistentHash().getNumSegments();

      return Flowable.fromCompletionStage(prefetchAllBucketHashesAsync(topology))
            .flatMap(prefetchedHashes ->
                  Flowable.range(0, totalSegments)
                        .flatMap(segmentId -> processSegmentAsync(segmentId, topology, prefetchedHashes),
                              false, SEGMENT_CONCURRENCY)
            )
            .filter(map -> hasConflict(map))
            .timeout(conflictTimeout, TimeUnit.MILLISECONDS)
            .doOnError(t -> stateReceiver.cancelRequests())
            .doOnCancel(() -> stateReceiver.cancelRequests())
            .doFinally(() -> streamInProgress.set(false));
   }

   private Flowable<Map<Address, CacheEntry<K, V>>> processSegmentAsync(
         int segmentId, LocalizedCacheTopology topology,
         Map<Integer, Map<Address, List<BucketHash>>> prefetchedHashes) {
      IntSet mismatchedBuckets = findMismatchedBuckets(segmentId, topology, prefetchedHashes);

      if (mismatchedBuckets != null && mismatchedBuckets.isEmpty()) {
         return Flowable.empty();
      }

      if (mismatchedBuckets != null) {
         if (log.isTraceEnabled())
            log.tracef("Cache %s segment %s fetching entries from %d mismatched buckets: %s",
                  cacheName, segmentId, mismatchedBuckets.size(), mismatchedBuckets);
         return Flowable.fromCompletionStage(
                     getReplicasForBucketsAsync(segmentId, mismatchedBuckets, topology))
               .flatMapIterable(list -> list)
               .onErrorResumeNext(t -> {
                  if (log.isTraceEnabled())
                     log.tracef("Cache %s segment %s bucket fetch failed, falling back: %s",
                           cacheName, segmentId, t.getMessage());
                  return fullSegmentFetchFlowable(segmentId, topology);
               });
      }

      if (log.isTraceEnabled())
         log.tracef("Cache %s segment %s hash comparison failed, falling back to full fetch with topology %s",
               cacheName, segmentId, topology);
      return fullSegmentFetchFlowable(segmentId, topology);
   }

   private Flowable<Map<Address, CacheEntry<K, V>>> fullSegmentFetchFlowable(
         int segmentId, LocalizedCacheTopology topology) {
      return Flowable.fromCompletionStage(
                  stateReceiver.getAllReplicasForSegment(segmentId, topology, conflictTimeout))
            .flatMapIterable(list -> list);
   }

   @Override
   public boolean isConflictResolutionInProgress() {
      return streamInProgress.get();
   }

   @Override
   public void resolveConflicts() {
      if (entryMergePolicy == null)
         throw new CacheException("Cannot resolve conflicts as no EntryMergePolicy has been configured");

      resolveConflicts(entryMergePolicy);
   }

   @Override
   public void resolveConflicts(EntryMergePolicy<K, V> mergePolicy) {
      checkIsRunning();
      try {
         resolveConflictsAsync(mergePolicy).toCompletableFuture().join();
      } catch (CompletionException e) {
         Throwable cause = e.getCause();
         if (cause instanceof CacheException) throw (CacheException) cause;
         throw new CacheException(cause);
      }
   }

   @Override
   public CompletionStage<Void> resolveConflictsAsync() {
      if (entryMergePolicy == null)
         throw new CacheException("Cannot resolve conflicts as no EntryMergePolicy has been configured");

      return resolveConflictsAsync(entryMergePolicy);
   }

   @Override
   public CompletionStage<Void> resolveConflictsAsync(EntryMergePolicy<K, V> mergePolicy) {
      checkIsRunning();
      return doResolveConflictsAsync(distributionManager.getCacheTopology(), mergePolicy, null);
   }

   @Override
   public CompletionStage<Void> resolveConflicts(CacheTopology topology, Set<Address> preferredNodes) {
      if (!running)
         return CompletableFuture.completedFuture(null);

      LocalizedCacheTopology localizedTopology;
      if (topology instanceof LocalizedCacheTopology) {
         localizedTopology = (LocalizedCacheTopology) topology;
      } else {
         localizedTopology = distributionManager.createLocalizedCacheTopology(topology);
      }
      conflictFuture = doResolveConflictsAsync(localizedTopology, entryMergePolicy, preferredNodes)
            .toCompletableFuture();
      return conflictFuture.whenComplete((v, t) -> {
         if (t != null) {
            streamInProgress.set(false);
         }
      });
   }

   @Override
   public void cancelConflictResolution() {
      if (conflictFuture != null && !conflictFuture.isDone()) {
         if (log.isTraceEnabled()) log.tracef("Cache %s cancelling conflict resolution future", cacheName);
         conflictFuture.cancel(true);
      }
      streamInProgress.set(false);
   }

   private CompletionStage<Void> doResolveConflictsAsync(final LocalizedCacheTopology topology,
                                                         final EntryMergePolicy<K, V> mergePolicy,
                                                         final Set<Address> preferredNodes) {
      boolean userCall = preferredNodes == null;
      final Set<Address> preferredPartition = userCall ? new HashSet<>(topology.getCurrentCH().getMembers()) : preferredNodes;

      if (log.isTraceEnabled())
         log.tracef("Cache %s attempting to resolve conflicts.  All Members %s, Installed topology %s, Preferred Partition %s",
               cacheName, topology.getMembers(), topology, preferredPartition);

      return getConflictsFlowable(topology)
            .flatMapCompletable(conflictMap -> {
               if (log.isTraceEnabled()) log.tracef("Cache %s conflict detected %s", cacheName, conflictMap);

               K key = null;
               for (CacheEntry<K, V> ce : conflictMap.values()) {
                  if (!(ce instanceof NullCacheEntry)) {
                     key = ce.getKey();
                     break;
                  }
               }
               if (key == null) {
                  throw new CacheException("All returned conflicts are NullCacheEntries. This should not happen!");
               }
               final K conflictKey = key;

               Address primaryReplica = topology.getDistribution(conflictKey).primary();

               Address singlePreferred = null;
               int preferredCount = 0;
               for (Address addr : conflictMap.keySet()) {
                  if (preferredPartition.contains(addr)) {
                     singlePreferred = addr;
                     if (++preferredCount > 1) break;
                  }
               }

               CacheEntry<K, V> preferredEntry;
               if (preferredCount == 1) {
                  preferredEntry = conflictMap.remove(singlePreferred);
               } else {
                  preferredEntry = conflictMap.remove(primaryReplica);
               }

               if (log.isTraceEnabled())
                  log.tracef("Cache %s applying EntryMergePolicy %s to PreferredEntry %s, otherEntries %s",
                        cacheName, mergePolicy.getClass().getName(), preferredEntry, conflictMap.values());

               CacheEntry<K, V> entry = preferredEntry instanceof NullCacheEntry ? null : preferredEntry;
               List<CacheEntry<K, V>> otherEntries = new ArrayList<>(conflictMap.size());
               for (CacheEntry<K, V> ce : conflictMap.values()) {
                  if (!(ce instanceof NullCacheEntry)) {
                     otherEntries.add(ce);
                  }
               }
               CacheEntry<K, V> mergedEntry = mergePolicy.merge(entry, otherEntries);

               CompletableFuture<V> future = applyMergeResult(userCall, conflictKey, mergedEntry);
               return Completable.fromCompletionStage(future)
                     .onErrorComplete(t -> {
                        log.exceptionDuringConflictResolution(conflictKey, t);
                        return true;
                     });
            })
            .toCompletionStage(null)
            .thenRun(() -> {
               if (log.isTraceEnabled())
                  log.tracef("Cache %s finished resolving conflicts for topologyId=%s", cacheName, topology.getTopologyId());
            });
   }

   private CompletableFuture<V> applyMergeResult(boolean userCall, K key, CacheEntry<K, V> mergedEntry) {
      long flags = userCall ? userMergeFlags : autoMergeFlags;
      VisitableCommand command;
      if (mergedEntry == null) {
         if (log.isTraceEnabled()) log.tracef("Cache %s executing remove on conflict: key %s", cacheName, key);
         command = commandsFactory.buildRemoveCommand(key, null, keyPartitioner.getSegment(key), flags);
      } else {
         if (log.isTraceEnabled())
            log.tracef("Cache %s executing update on conflict: key %s with value %s", cacheName, key, mergedEntry
                  .getValue());
         command = commandsFactory.buildPutKeyValueCommand(key, mergedEntry.getValue(), keyPartitioner.getSegment(key),
               mergedEntry.getMetadata(), flags);
      }
      try {
         assert transactionManager == null || transactionManager.getTransaction() == null : "Transaction active on conflict resolution thread";
         InvocationContext ctx = invocationHelper.createInvocationContextWithImplicitTransaction(1, true);
         return invocationHelper.invokeAsync(ctx, command);
      } catch (Exception e) {
         return CompletableFuture.failedFuture(e);
      }
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.running().isStateTransferInProgress();
   }

   private void checkIsRunning() {
      if (!running)
         throw new CacheException(String.format("Cache %s unable to process request as the ConflictManager has been stopped", cacheName));
   }

   private class VersionRequest {
      final K key;
      final boolean postpone;
      final CompletableFuture<Map<Address, InternalCacheValue<V>>> completableFuture = new CompletableFuture<>();
      volatile CompletableFuture<Map<Address, Response>> rpcFuture;
      volatile Collection<Address> keyOwners;

      VersionRequest(K key, boolean postpone) {
         this.key = key;
         this.postpone = postpone;

         if (log.isTraceEnabled()) log.tracef("Cache %s creating %s", cacheName, this);

         if (postpone) {
            retryQueue.add(this);
         } else {
            start();
         }
      }

      void cancelRequestIfOutdated() {
         Collection<Address> latestOwners = distributionManager.getCacheTopology().getWriteOwners(key);
         if (rpcFuture != null && !completableFuture.isDone() && !keyOwners.equals(latestOwners)) {
            rpcFuture = null;
            keyOwners.clear();
            if (rpcFuture.cancel(false)) {
               retryQueue.add(this);

               if (log.isTraceEnabled())
                  log.tracef("Cancelling %s for nodes %s. New write owners %s", this, keyOwners, latestOwners);
            }
         }
      }

      void start() {
         LocalizedCacheTopology topology = distributionManager.getCacheTopology();
         DistributionInfo info = topology.getDistribution(key);
         keyOwners = info.writeOwners();

         if (log.isTraceEnabled()) log.tracef("Attempting %s from owners %s", this, keyOwners);

         final Map<Address, InternalCacheValue<V>> versionsMap = new HashMap<>();
         if (keyOwners.contains(localAddress)) {
            GetCacheEntryCommand cmd = commandsFactory.buildGetCacheEntryCommand(key, info.segmentId(), localFlags);
            InvocationContext ctx = invocationContextFactory.createNonTxInvocationContext();
            CacheEntry<K, V> entry = (CacheEntry<K, V>) interceptorChain.running().invoke(ctx, cmd);
            InternalCacheValue<V> icv = entry != null ? internalEntryFactory.createValue(entry) : null;
            synchronized (versionsMap) {
               versionsMap.put(localAddress, icv);
            }
         }

         ClusteredGetCommand cmd = commandsFactory.buildClusteredGetCommand(key, info.segmentId(), FlagBitSets.SKIP_OWNERSHIP_CHECK);
         cmd.setTopologyId(topology.getTopologyId());
         MapResponseCollector collector = MapResponseCollector.ignoreLeavers(keyOwners.size());
         rpcFuture = rpcManager.invokeCommand(keyOwners, cmd, collector, rpcManager.getSyncRpcOptions()).toCompletableFuture();
         rpcFuture.whenComplete((responseMap, exception) -> {
            if (log.isTraceEnabled())
               log.tracef("%s received responseMap %s, exception %s", this, responseMap, exception);

            if (exception != null) {
               String msg = String.format("%s encountered when attempting '%s' on cache '%s'", exception.getCause(), this, cacheName);
               completableFuture.completeExceptionally(new CacheException(msg, exception.getCause()));
               return;
            }

            for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
               if (log.isTraceEnabled())
                  log.tracef("%s received response %s from %s", this, entry.getValue(), entry.getKey());
               Response rsp = entry.getValue();
               if (rsp instanceof SuccessfulResponse) {
                  SuccessfulResponse response = (SuccessfulResponse) rsp;
                  Object rspVal = response.getResponseValue();
                  synchronized (versionsMap) {
                     versionsMap.put(entry.getKey(), (InternalCacheValue<V>) rspVal);
                  }
               } else if (rsp instanceof UnsureResponse) {
                  log.debugf("Received UnsureResponse, restarting request %s", this);
                  this.start();
                  return;
               } else if (rsp instanceof CacheNotFoundResponse) {
                  if (log.isTraceEnabled()) log.tracef("Ignoring CacheNotFoundResponse: %s", rsp);
               } else {
                  completableFuture.completeExceptionally(new CacheException(String.format("Unable to retrieve key %s from %s: %s", key, entry.getKey(), entry.getValue())));
                  return;
               }
            }
            completableFuture.complete(versionsMap);
         });
      }

      @Override
      public String toString() {
         return "VersionRequest{" +
               "key=" + key +
               ", postpone=" + postpone +
               '}';
      }
   }

   private static boolean hasConflict(Map<?, ?> map) {
      if (map.isEmpty()) return true;
      Object first = null;
      for (Object v : map.values()) {
         if (first == null) {
            first = v;
         } else if (!first.equals(v)) {
            return true;
         }
      }
      return false;
   }

   /**
    * Prefetches bucket hashes for all segments from all write owners using batched RPCs.
    * Sends one RPC per remote node, amortizing network latency. Returns an empty map on failure
    * (callers fall back to full segment fetch when hash data is missing).
    */
   @SuppressWarnings("unchecked")
   private CompletionStage<Map<Integer, Map<Address, List<BucketHash>>>> prefetchAllBucketHashesAsync(
         LocalizedCacheTopology topology) {
      try {
         int totalSegments = topology.getWriteConsistentHash().getNumSegments();
         SegmentHasher hasher = new SegmentHasher(dataContainer, internalMarshaller);
         Map<Integer, Map<Address, List<BucketHash>>> result = new HashMap<>();

         Map<Address, IntSet> remoteOwnerSegments = new HashMap<>();
         IntSet localSegments = IntSets.mutableEmptySet(totalSegments);

         for (int seg = 0; seg < totalSegments; seg++) {
            List<Address> writeOwners = topology.getSegmentDistribution(seg).writeOwners();
            if (writeOwners.size() <= 1) continue;

            if (writeOwners.contains(localAddress)) {
               localSegments.set(seg);
            }
            for (Address addr : writeOwners) {
               if (!addr.equals(localAddress)) {
                  remoteOwnerSegments.computeIfAbsent(addr,
                        a -> IntSets.mutableEmptySet(totalSegments)).set(seg);
               }
            }
         }

         if (!localSegments.isEmpty()) {
            List<BucketHash> localHashes = hasher.computeAllBucketHashes(localSegments, BUCKET_COUNT);
            for (BucketHash bh : localHashes) {
               result.computeIfAbsent(bh.segmentId(), s -> new HashMap<>())
                     .computeIfAbsent(localAddress, a -> new ArrayList<>())
                     .add(bh);
            }
         }

         if (remoteOwnerSegments.isEmpty()) {
            return CompletableFuture.completedFuture(result);
         }

         List<CompletableFuture<Void>> rpcFutures = new ArrayList<>();
         for (Map.Entry<Address, IntSet> entry : remoteOwnerSegments.entrySet()) {
            Address remoteAddr = entry.getKey();
            GetBucketHashesCommand cmd = commandsFactory.buildGetBucketHashesCommand(
                  topology.getTopologyId(), entry.getValue(), BUCKET_COUNT);
            CompletableFuture<Void> rpcFuture = rpcManager.invokeCommand(
                        List.of(remoteAddr), cmd,
                        MapResponseCollector.ignoreLeavers(1),
                        rpcManager.getSyncRpcOptions())
                  .toCompletableFuture()
                  .thenAccept(responseMap -> {
                     Response rsp = responseMap.get(remoteAddr);
                     if (!(rsp instanceof SuccessfulResponse)) return;
                     List<BucketHash> remoteBuckets = (List<BucketHash>) ((SuccessfulResponse<?>) rsp).getResponseValue();
                     if (remoteBuckets == null) return;

                     synchronized (result) {
                        for (BucketHash bh : remoteBuckets) {
                           result.computeIfAbsent(bh.segmentId(), s -> new HashMap<>())
                                 .computeIfAbsent(remoteAddr, a -> new ArrayList<>())
                                 .add(bh);
                        }
                     }
                  });
            rpcFutures.add(rpcFuture);
         }

         return CompletableFuture.allOf(rpcFutures.toArray(CompletableFuture[]::new))
               .thenApply(v -> {
                  if (log.isTraceEnabled())
                     log.tracef("Cache %s prefetched bucket hashes for %d segments from %d remote nodes",
                           cacheName, result.size(), remoteOwnerSegments.size());
                  return result;
               })
               .exceptionally(t -> {
                  if (log.isTraceEnabled())
                     log.tracef("Cache %s bucket hash prefetch failed: %s", cacheName, t.getMessage());
                  return Collections.emptyMap();
               });
      } catch (Exception e) {
         if (log.isTraceEnabled())
            log.tracef("Cache %s bucket hash prefetch failed: %s", cacheName, e.getMessage());
         return CompletableFuture.completedFuture(Collections.emptyMap());
      }
   }

   /**
    * Compares bucket hashes for a segment using prefetched data (no RPCs).
    * <p>
    * Returns:
    * <ul>
    *   <li>Empty set — all hashes match, segment can be skipped</li>
    *   <li>Non-empty set — IDs of mismatched buckets</li>
    *   <li>{@code null} — data unavailable, caller should fall back to full segment fetch</li>
    * </ul>
    */
   private IntSet findMismatchedBuckets(int segmentId, LocalizedCacheTopology topology,
                                        Map<Integer, Map<Address, List<BucketHash>>> prefetchedHashes) {
      List<Address> writeOwners = topology.getSegmentDistribution(segmentId).writeOwners();
      if (writeOwners.size() <= 1) return IntSets.immutableEmptySet();

      if (prefetchedHashes == null) return null;

      Map<Address, List<BucketHash>> segmentHashes = prefetchedHashes.get(segmentId);
      if (segmentHashes == null || segmentHashes.size() < 2) return null;

      // Validate bucket counts
      for (List<BucketHash> buckets : segmentHashes.values()) {
         if (buckets.size() != BUCKET_COUNT) return null;
      }

      // Compare segment-level hashes (derived from bucket hashes) and track max entry count
      SegmentHash referenceHash = null;
      boolean segmentMismatch = false;
      int maxEntries = 0;
      for (List<BucketHash> buckets : segmentHashes.values()) {
         SegmentHash sh = SegmentHasher.deriveSegmentHash(segmentId, buckets);
         maxEntries = Math.max(maxEntries, sh.entryCount());
         if (referenceHash == null) {
            referenceHash = sh;
         } else if (!referenceHash.matches(sh)) {
            segmentMismatch = true;
         }
      }

      if (!segmentMismatch) {
         if (log.isTraceEnabled())
            log.tracef("Cache %s segment %s hashes match across all write owners, skipping",
                  cacheName, segmentId);
         return IntSets.immutableEmptySet();
      }

      // Small segment — skip per-bucket narrowing, fetch all entries directly
      if (maxEntries <= SMALL_SEGMENT_THRESHOLD) {
         if (log.isTraceEnabled())
            log.tracef("Cache %s segment %s is small (%d entries max), fetching all entries",
                  cacheName, segmentId, maxEntries);
         return ALL_BUCKETS;
      }

      // Large segment — identify which buckets differ
      List<List<BucketHash>> allBucketLists = new ArrayList<>(segmentHashes.values());
      IntSet mismatched = IntSets.mutableEmptySet(BUCKET_COUNT);
      for (int b = 0; b < BUCKET_COUNT; b++) {
         BucketHash ref = allBucketLists.get(0).get(b);
         for (int i = 1; i < allBucketLists.size(); i++) {
            if (!ref.matches(allBucketLists.get(i).get(b))) {
               mismatched.set(b);
               break;
            }
         }
      }

      if (log.isTraceEnabled())
         log.tracef("Cache %s segment %s bucket hash comparison: %d of %d buckets mismatched",
               cacheName, segmentId, mismatched.size(), BUCKET_COUNT);

      return mismatched;
   }

   /**
    * Fetches entries only from mismatched buckets and groups them by key with NullCacheEntry,
    * matching the same output format as StateReceiverImpl.getAllReplicasForSegment().
    */
   @SuppressWarnings("unchecked")
   private CompletionStage<List<Map<Address, CacheEntry<K, V>>>> getReplicasForBucketsAsync(
         int segmentId, IntSet bucketIds, LocalizedCacheTopology topology) {
      List<Address> writeOwners = topology.getSegmentDistribution(segmentId).writeOwners();
      boolean allBuckets = bucketIds.size() >= BUCKET_COUNT;
      Map<K, Map<Address, CacheEntry<K, V>>> keyReplicaMap = new HashMap<>();

      if (writeOwners.contains(localAddress)) {
         SegmentHasher hasher = allBuckets ? null : new SegmentHasher(dataContainer, internalMarshaller);
         Iterator<InternalCacheEntry<K, V>> it = dataContainer.iterator(IntSets.immutableSet(segmentId));
         while (it.hasNext()) {
            InternalCacheEntry<K, V> entry = it.next();
            if (allBuckets || bucketIds.contains(hasher.bucketForKey(entry.getKey(), BUCKET_COUNT))) {
               addToReplicaMap(keyReplicaMap, localAddress, entry, writeOwners);
            }
         }
      }

      List<Address> remoteOwners = writeOwners.stream()
            .filter(a -> !a.equals(localAddress)).collect(Collectors.toList());
      if (remoteOwners.isEmpty()) {
         return CompletableFuture.completedFuture(new ArrayList<>(keyReplicaMap.values()));
      }

      GetBucketEntriesCommand cmd = commandsFactory.buildGetBucketEntriesCommand(
            topology.getTopologyId(), segmentId, bucketIds, BUCKET_COUNT);
      MapResponseCollector collector = MapResponseCollector.ignoreLeavers(remoteOwners.size());
      return rpcManager.invokeCommand(remoteOwners, cmd, collector, rpcManager.getSyncRpcOptions())
            .thenApply(responseMap -> {
               for (Map.Entry<Address, Response> rspEntry : responseMap.entrySet()) {
                  Response rsp = rspEntry.getValue();
                  if (!(rsp instanceof SuccessfulResponse))
                     throw new CacheException("Unexpected response from " + rspEntry.getKey());
                  List<CacheEntry<K, V>> entries = (List) ((SuccessfulResponse<?>) rsp).getResponseValue();
                  for (CacheEntry<K, V> entry : entries) {
                     addToReplicaMap(keyReplicaMap, rspEntry.getKey(), entry, writeOwners);
                  }
               }
               return new ArrayList<>(keyReplicaMap.values());
            });
   }

   private void addToReplicaMap(Map<K, Map<Address, CacheEntry<K, V>>> keyReplicaMap,
                                Address address, CacheEntry<K, V> entry, List<Address> writeOwners) {
      keyReplicaMap.computeIfAbsent(entry.getKey(), k -> {
         Map<Address, CacheEntry<K, V>> map = new HashMap<>();
         writeOwners.forEach(a -> map.put(a, NullCacheEntry.getInstance()));
         return map;
      }).put(address, entry);
   }

}
