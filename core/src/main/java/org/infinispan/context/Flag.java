package org.infinispan.context;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Available flags, which may be set on a per-invocation basis.  These are
 * provided using the {@link AdvancedCache} interface, using some of the
 * overloaded methods that allow passing in of a variable number of Flags.
 *
 * When making modifications to these enum, do not change the order of
 * enumerations, so always append any new enumerations after the last one.
 * Finally, enumerations should not be removed.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.FLAG)
public enum Flag {

   /**
    * Overrides the {@link org.infinispan.configuration.cache.LockingConfiguration#lockAcquisitionTimeout(long)} configuration setting by ensuring lock
    * managers use a 0-millisecond lock acquisition timeout.  Useful if you only want to acquire a lock on an entry
    * <i>if and only if</i> the lock is uncontended.
    */
   @ProtoEnumValue(number = 1)
   ZERO_LOCK_ACQUISITION_TIMEOUT,

   /**
    * Forces LOCAL mode operation, even if the cache is configured to use a clustered mode like replication,
    * invalidation or distribution. Applying this flag will suppress any RPC messages otherwise associated with this
    * invocation. Write operations mat not acquire the entry locks. In distributed mode, the modifications performed
    * during an operation in a non-owner node are going to L1, if it is enabled, otherwise the operation is a no-op in
    * that node.
    */
   @ProtoEnumValue(number = 2)
   CACHE_MODE_LOCAL,

   /**
    * Bypasses lock acquisition for this invocation altogether. A potentially dangerous flag, as it can lead to
    * inconsistent data: a Lock is needed to make sure the same value is written to each node replica; a lock
    * is also needed to guarantee that several writes on the same key are not applied out of order to an async CacheLoader
    * storage engine.
    * So this flag is useful only as an optimization when the same key is written once and never again, or as
    * an unsafe optimisation if the period between writes on the same key is large enough to make a race condition
    * never happen in practice. If this is unclear, avoid it.
    */
   @ProtoEnumValue(number = 3)
   SKIP_LOCKING,

   /**
    * Forces a write lock, even if the invocation is a read operation.  Useful when reading an entry to later update it
    * within the same transaction, and is analogous in behavior and use case to a <tt>select ... for update ... </tt>
    * SQL statement.
    */
   @ProtoEnumValue(number = 4)
   FORCE_WRITE_LOCK,

   /**
    * Forces asynchronous network calls where possible, even if otherwise configured to use synchronous network calls.
    * Only applicable to non-local, clustered caches.
    */
   @ProtoEnumValue(number = 5)
   FORCE_ASYNCHRONOUS,

   /**
    * Forces synchronous network calls where possible, even if otherwise configured to use asynchronous network calls.
    * Only applicable to non-local, clustered caches.
    */
   @ProtoEnumValue(number = 6)
   FORCE_SYNCHRONOUS,

   /**
    * Skips storing an entry to any configured {@link org.infinispan.persistence.spi.CacheLoader}s.
    */
   @ProtoEnumValue(number = 7)
   SKIP_CACHE_STORE,

   /**
    * Skips loading an entry from any configured {@link org.infinispan.persistence.spi.CacheLoader}s.
    * Useful for example to perform a {@link Cache#put(Object, Object)} operation while not interested
    * in the return value (i.e. the previous value of the key).
    * <br>
    * Note that the loader will be skipped even if that changes the meaning of the operation, e.g. for
    * conditional write operations. If that is not intended,
    * you should use {@link #IGNORE_RETURN_VALUES} instead.
    */
   @ProtoEnumValue(number = 8)
   SKIP_CACHE_LOAD,

   /**
    * <p>Swallows any exceptions, logging them instead at a low log level.  Will prevent a failing operation from
    * affecting any ongoing JTA transactions as well.</p>
    * <p>This Flag will not be replicated to remote nodes, but it will still protect the invoker from remote exceptions.</p>
    * <p>When using this flag with Optimistic caches, lock acquisition happen in the prepare phase at which
    * point this flag will be ignored in order to ensure that Infinispan reports the correct exception
    * back to the transaction manager. This is done for safety reasons to avoid inconsistent cache contents.</p>
    */
   @ProtoEnumValue(number = 9)
   FAIL_SILENTLY,

   /**
    * When used with <b>distributed</b> cache mode, will prevent retrieving a remote value either when
    * executing a {@link Cache#get(Object)} or {@link Cache#containsKey(Object)},
    * or to return the overwritten value for {@link Cache#put(Object, Object)} or {@link Cache#remove(Object)}.
    * This would render return values for most operations unusable, in exchange for the performance gains of
    * reducing remote calls.
    * <br>
    * Note that the remote lookup will be skipped even if that changes the meaning of the operation, e.g. for
    * conditional write operations. If that is not intended,
    * you should use {@link #IGNORE_RETURN_VALUES} instead.
    */
   @ProtoEnumValue(number = 10)
   SKIP_REMOTE_LOOKUP,

   /**
    * Used by the Query module only, it will prevent the indexes to be updated as a result of the current operations.
    */
   @ProtoEnumValue(number = 11)
   SKIP_INDEXING,

   /**
    * Flags the invocation as a {@link Cache#putForExternalRead(Object, Object)}
    * call, as opposed to a regular {@link Cache#put(Object, Object)}. This
    * flag was created purely for internal Infinispan usage, and should not be
    * used by clients calling into Infinispan.
    */
   @ProtoEnumValue(number = 12)
   PUT_FOR_EXTERNAL_READ,

   /**
    * Flags the invocation as a put operation done internally by the state transfer.
    * This flag was created purely for internal Infinispan usage, and should not be
    * used by clients calling into Infinispan.
    *
    * Note for internal users: PUT_FOR_STATE_TRANSFER only applies to state transfer-specific actions,
    * in order to avoid loading the previous value one should add the IGNORE_RETURN_VALUES flag explicitly.
    */
   @ProtoEnumValue(number = 13)
   PUT_FOR_STATE_TRANSFER,

   /**
    * Flags the invocation as a put operation done internally by the cross-site state transfer. This flag was created
    * purely for internal Infinispan usage, and should not be used by clients calling into Infinispan.
    */
   @ProtoEnumValue(number = 14)
   PUT_FOR_X_SITE_STATE_TRANSFER,

   /**
    * If this flag is enabled, if a cache store is shared, then storage to the store is skipped.
    */
   @ProtoEnumValue(number = 15)
   SKIP_SHARED_CACHE_STORE,

   /**
    * Ignore current consistent hash and read from data container/commit the change no matter what (if the flag is set).
    */
   @ProtoEnumValue(number = 16)
   SKIP_OWNERSHIP_CHECK,

   /**
    * Signals that a write operation's return value will be ignored, so reading
    * the existing value from a store or from a remote node is not necessary.
    *
    * Typical operations whose return value might be ignored include
    * {@link java.util.Map#put(Object, Object)} whose return value indicates
    * previous value. So, a user might decide to the put something in the
    * cache but might not be interested in the return value.
    *
    * This flag is ignored for operations that need the existing value to execute
    * correctly, e.g. {@link Cache#get(Object)},
    * conditional remove ({@link Cache#remove(Object, Object)}),
    * and replace with an expected value ({@link Cache#replace(Object, Object, Object)}).
    *
    * That means it is safe to use {@code IGNORE_RETURN_VALUES} for all the operations on a cache,
    * unlike {@link Flag#SKIP_REMOTE_LOOKUP} and {@link Flag#SKIP_CACHE_LOAD}.
    */
   @ProtoEnumValue(number = 17)
   IGNORE_RETURN_VALUES,

   /**
    * If cross-site replication is enabled, this would skip the replication to any remote site.
    */
   @ProtoEnumValue(number = 18)
   SKIP_XSITE_BACKUP,

   /**
    * This flag skips listener notifications as a result of a cache operation.
    * For example, if this flag is passed as a result of a {@link Cache#get(Object)}
    * call, no callbacks will be made on listeners annotated with
    * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited}.
    */
   @ProtoEnumValue(number = 19)
   SKIP_LISTENER_NOTIFICATION,

   /**
    * This flag skips statistics updates as a result of a cache operation.
    * For example, if this flag is passed as a result of a {@link Cache#get(Object)}
    * call, no cache hits or cache miss counters will be updated.
    */
   @ProtoEnumValue(number = 20)
   SKIP_STATISTICS,

   /**
    * Any time a new indexed entry is inserted, a delete statement is issued on the indexes
    * to remove previous values. This delete statement is executed even if there is no known
    * entry having the same key. Enable this flag when you know for sure there is no existing
    * entry in the index for improved performance.
    * For example, this is useful for speeding up import of new data in an empty cache
    * having an empty index.
    */
   @ProtoEnumValue(number = 21)
   SKIP_INDEX_CLEANUP,

   /**
    * If a write operation encounters a retry due to a topology change this flag should be used to tell the new owner
    * that such a thing happened.  This flag was created purely for internal Infinispan usage, and should not be
    * used by clients calling into Infinispan.
    */
   @ProtoEnumValue(number = 22)
   COMMAND_RETRY,

   /**
    * Flag to identity that data is being written as part of a Rolling Upgrade.
    */
   @ProtoEnumValue(number = 23)
   ROLLING_UPGRADE,

   /**
    * Flag to identify that this iteration is done on a remote node and thus no additional wrappings are required
    * @deprecated Since 14.0, no longer does anything. Will be removed in 17.0.
    */
   @Deprecated(forRemoval=true, since = "14.0")
   @ProtoEnumValue(number = 24)
   REMOTE_ITERATION,

   /**
    * Flag that can be used to skip any size optimizations - forcing iteration of entries to count. User shouldn't
    * normally need to use this flag. This is helpful if there are concerns that can cause just a simple size invocation
    * from being consistent (eg. on-going transaction with modifications).
    */
   @ProtoEnumValue(number = 25)
   SKIP_SIZE_OPTIMIZATION,

   /**
    * Flag that is used by keySet, entrySet and values methods so that they do not return transactional values. Normally
    * an end user would not need to do this.
    */
   @ProtoEnumValue(number = 26)
   IGNORE_TRANSACTION,

   /**
    * Signals a {@link org.infinispan.commands.write.WriteCommand} as an update from a remote site (async).
    * <p>
    * Internal use
    */
   @ProtoEnumValue(number = 27)
   IRAC_UPDATE,

   /**
    * Signals a {@link org.infinispan.commands.write.WriteCommand} as state transfer from remote site.
    * <p>
    * Internal use
    */
   @ProtoEnumValue(number = 28)
   IRAC_STATE,

   /**
    * Flag to designate that this operation was performed on behalf of another that already has the lock for the given
    * key.
    */
   @ProtoEnumValue(number = 29)
   ALREADY_HAS_LOCK,

   /**
    * Signals that a {@link org.infinispan.commands.write.WriteCommand} was sent from the primary as a backup operation.
    * Some things do not need to be checked in this case.
    */
   @ProtoEnumValue(number = 30)
   BACKUP_WRITE,

   /**
    * Signals that a state transfer is in course. This is primarily used to identify how to load data from cache stores
    * during the state transfer.
    */
   @ProtoEnumValue(number = 31)
   STATE_TRANSFER_PROGRESS
}
