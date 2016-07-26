/**
 * <h1>SCATTERED CACHE DESIGN</h1>
 *
 * <h2>Idea</h2>
 *
 * Distributed caches have fixed owners for each key. Operations where originator is one of the owners
 * require less messages (and possibly less roundtrips), so let's make the originator always the owner.
 * In case of node crash, we can retrieve the data by inspecting all nodes.
 * To find the last-written entry in case of crashed primary owner, entries will keep write versions
 * in metadata. These versions also determine order of writes; we don't have to use locking anymore.
 * <p>
 * Pros:
 * <ul>
 *    <li>faster writes
 *    <li>no locking contention
 * </ul>
 * <p>
 * Cons:
 * <ul>
 *    <li>reads always have to go to the primary (slower writes in small clusters)
 *    <li>complex reconciliation during rebalance
 * </ul>
 *
 * <h2>Scope of implementation</h2>
 * <ul>
 *    <li>Scattered cache is implemented as resilient to single node failure (equivalent to 2 owners
 *        distributed cache).
 *    <li>Transactional mode is not implemented yet.
 *    <li>Functional commands are not fully implemented yet.
 *    <li>All other features (state transfer, distributed streams, persistence...) should work.
 * </ul>
 *
 * <h2>Operations</h2>
 *
 * We need to keep tombstones with versions after entry removal. These tombstones have limited lifespan -
 * we keep them around only until the invalidations are applied on all nodes.
 * <p>
 * The versions have to grow monotonically; therefore the version counter won't be per-entry but per segment
 * (as tombstone will be eventually removed, per-entry version would be lost). The version is implemented by
 * {@link org.infinispan.container.versioning.SimpleClusteredVersion} and therefore it contains topology id.
 * <p>
 * Unlike other cache modes, entry commit does not happen in {@link org.infinispan.interceptors.impl.EntryWrappingInterceptor}
 * but before replication to backup in {@link org.infinispan.interceptors.distribution.ScatteredDistributionInterceptor}
 * (see below for the detailed operation descriptions). As scattered cache synchronizes only on the data container
 * (instead of using locking interceptors), the value in data container can change between loading that in
 * {@link org.infinispan.interceptors.impl.EntryWrappingInterceptor} and committing it. Therefore, for command
 * that reads previous values according to {@link org.infinispan.commands.write.WriteCommand#loadType()}
 * the version seen before modification is checked against actual data-container value and if it does not match,
 * {@link org.infinispan.interceptors.distribution.ConcurrentChangeException} is thrown. This is caught in
 * {@link org.infinispan.interceptors.impl.RetryingEntryWrappingInterceptor} and the command is retried in that case.
 *
 * <h3>Single entry write (put, getAndPut, putIfAbsent, replace...)</h3>
 *
 * <h4>originator == primary owner</h4>
 * <ol>
 *    <li>Primary increments version for segment
 *    <li>Primary commits entry
 *    <li>Primary picks one node (next member in CH) and sends backup RPC
 *    <li>Backup commits entry
 *    <li>Backup sends RPC response
 *    <li>Primary returns after it gets the response
 *    <li>Primary schedules invalidation of entry with lower versions
 * </ol>
 * <p>
 * Selection of backup could be random, but having it ~fixed probably reduces overall memory consumption
 * <p>
 * Updating value on primary before backup finishes does not change data consistency - if backup RPC fails
 *     in distributed cache we can't know whether backup has committed the entry and so it can be published anyway.
 *
 * <h4>originator != primary owner</h4>
 * <ol>
 *    <li>Origin sends sync RPC to primary owner
 *    <li>Primary increments version for segment
 *    <li>Primary commits entry
 *    <li>Primary returns response with version (+ return value if appropriate)
 *    <li>Origin commits entry
 *    <li>Origin schedules invalidation of entry with lower versions
 * </ol>
 * <p>
 * Invalidation must be scheduled by origin, because primary does not know if backup committed
 *
 * <h3>Single entry read</h3>
 *
 * <h4>originator == primary owner</h4>
 * Just local read
 *
 * <h4>originator != primary owner</h4>
 * ATM just invoke sync RPC to the primary owner
 * <p>
 * Possible improvement (not implemented yet)
 * <ol>
 *    <li>Origin locally loads entry with SKIP_CACHE_LOAD
 *    <li>Origin sends sync RPC including the version to primary
 *    <li>Primary compares version with it's own
 *    <ol>
 *       <li>If version matches, origin gets just successful response and returns locally-loaded value
 *       <li>If version does not match, value + version is sent back
 *    </ol>
 * </ol>
 *
 * Optional configuration options:
 * <ul>
 *    <li>Allow reading local values only (if present) - risk of stale reads
 *    <li>Store read value locally with expiration (L1 enabled) - as invalidations are broadcast anyway,
 *        there's not much overhead with that. This will still require RPC on read (unless stale reads
 *        are allowed) but not marshalling the value.
 * </ul>
 *
 * <h3>Multiple entries writes</h3>
 * <ol>
 *    <li>Increment version for primary-owned entries and commit them
 *    <li>Backup these entries to next node
 *    <li>Send all other entries to their primary owner
 *    <li>Commit entries after successful response from primary
 * </ol>
 *
 * Possible improvement (not implemented yet):
 * <p>
 * Entries for which this node is the primary owner won't be backed up just to the next member,
 * but to a node that is primary owner of another entries in the multiwrite. That way some messages
 * can be spared by merging the primary(keyA) -> backup and origin -> primary(keyB) requests.
 *
 * <h3>Multiple entries reads</h3>
 *
 * Same as single entry reads, just merge RPCs for the same primary owners.
 *
 * <h3>Invalidations</h3>
 *
 * It would be inefficient to send invalidations (key + version) one-by-one, so these are be merged
 * and sent in batches, using {@link org.infinispan.commands.write.InvalidateVersionsCommand}.
 * <p>
 * Possible improvement (not implemented yet):
 * <p>
 * The list of invalidations-to-be-sent could be updated when we get invalidation from another node, in order
 * to reduce the overall noise.
 *
 * <h2>State Transfer</h2>
 *
 * During rebalance, scattered cache always uses pendinCH for both reads and writes. It does not implement four-phase
 * rebalance as the segment state and ability to read/write on a node is tracked in {@link org.infinispan.scattered.ScatteredVersionManager},
 * we use only two-phase rebalance.
 * <p>
 * When the command traverses through interceptor stack {@link org.infinispan.interceptors.impl.PrefetchInterceptor}
 * checks the segment state, and either retrieves the remove value (ahead of regular state transfer) or blocks
 * the command until the state transfer is finished (for commands which need all values - there's no need to start
 * a second retrieval of all values).
 * <p>
 * The state transfer of a segment has several phases:
 * <ol>
 *    <li>NOT_OWNED: this node is not primary owner, it can backup the entry, though
 *    <li>BLOCKED: node has just become an owner but the old owner did not revoke segment ownership yet
 *    <li>KEYS_TRANSFER: node knows what is the highest version for given segment and is requesting
 *        keys + versions (no values) from all other nodes.
 *    <li>VALUES_TRANSFER: we got all keys with metadata and now store the highest version of each key
 *        and the node storing the value in {@link org.infinispan.container.entries.RemoteMetadata}
 *    <li>OWNED: all data is in
 * </ol>
 * There are new types of {@link org.infinispan.statetransfer.StateRequestCommand}, namely it is:
 * <ul>
 *    <li>{@code CONFIRM_REVOKED_SEGMENTS} that makes sure that all old owners have adopted the new topology
 *        and won't serve furher requests according to the old one.
 *    <li>{@code START_KEYS_TRANSFER} that is very similar to {@code START_STATE_TRANSFER} but moves only keys.
 * </ul>
 * <p>
 * During node crash, we experience 3 topologies:
 * <ol>
 *    <li>CH_UPDATE just removing the dead member (STABLE topology)
 *    <li>REBALANCE starts shuffling entries around (TRANSITORY topology)
 *    <li>CH_UPDATE with final (STABLE) topology
 * </ol>
 * <p>
 * Operations are always driven by the new primary owner of given segment.
 *
 * <h4>If the segment has not changed an owner:</h4>
 * {@link org.infinispan.scattered.ScatteredStateProvider} does:
 * <ol>
 *    <li>Replicate all data from this segment to the next node using
 *        {@link org.infinispan.statetransfer.OutboundTransferTask#pushTransfer} {@code true}
 *    <li>Send {@link org.infinispan.commands.write.InvalidateVersionsCommand}s with all keys in this segment to all
 *        nodes but the next member (receiving state via the push transfer)
 * </ol>
 * Write to entry can proceed in parallel with this process; invalidation cannot overwrite newer entry,
 * though invalidation from executed write can arrive to the new backup before the state-transfer - then
 * the cluster would have 3 copies of that entry until next write as the entry would not be invalidated on backup.
 *
 * <h4>If the segment just got a new primary owner:</h4>
 * {@link org.infinispan.scattered.impl.ScatteredStateConsumerImpl} does:
 * <ol>
 *    <li>Synchronously retrieve highest version for this segment from all nodes (using {@code GET_MAX_VERSIONS}
 *    <li>Request all nodes to send you all keys + versions from this segment (and do that locally as well)
 *    <li>Retrieve values from nodes with highest versions
 *    <li>Send invalidations to all other nodes, removing the entry</li>
 * </ol>
 *
 * <h4>Clean rebalance (after join, no data is lost)</h4>
 *
 * Optimization for rebalance when there's single owner with all data (previous primary) has not been implemented yet.
 *
 * <h2>Partition handling</h2>
 * Partition becomes degraded any time it loses more than one node compared to members in last stable topology.
 * In degraded mode, all operations are prohibited; one partition cannot have all owners in (in that case operations
 * are allowed in distributed caches) because we don't know who is the backup owner. Having primary owner
 * is not sufficient; the other partition may be still available and therefore we would get inconsistent/provide
 * possibly stale data.
 *
 * <h2>Persistence</h2>
 * As we don't use locking for everything after {@link org.infinispan.interceptors.impl.EntryWrappingInterceptor}
 * we need another synchronization for storing an entry into cache store. We don't want to block data-container
 * for the potentially long cache store update, and therefore {@link org.infinispan.interceptors.impl.ScatteredCacheWriterInterceptor}
 * goes into data-container (getting the lock) just to compare versions and create a {@link java.util.concurrent.CompletableFuture}
 * that serves as a lock that can be waited upon in non-blocking way.
 * <p>
 *
 * <h2>Potential problems</h2>
 *
 * <h3>Listeners</h3>
 * <ul>
 *    <li>The pre- listeners may be invoked multiple times, with stale values (the command then does
 *        not update DC, and retries).
 *    <li>However if command itself does not read the value, it can commit even if the value changed
 *        in between and listener will get out-of-date value.
 *    <li>As ordering updates to DC is based on the versions, it is possible that some operations arrive to DC finding
 *        that a newer (according to version) update has been applied there. In that case, the operation correctly
 *        finishes, but an event for this update is not fired as we don't have the previous value, and the event that
 *        was fired for the newer update carries the value before this update.</li>
 * </ul>
 */
package org.infinispan.scattered;
