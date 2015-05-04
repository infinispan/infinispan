package org.infinispan.context;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.EnumSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.AdvancedCache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

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
public enum Flag {
   /**
    * Overrides the {@link Configuration#setLockAcquisitionTimeout(long)} configuration setting by ensuring lock
    * managers use a 0-millisecond lock acquisition timeout.  Useful if you only want to acquire a lock on an entry
    * <i>if and only if</i> the lock is uncontended.
    */
   ZERO_LOCK_ACQUISITION_TIMEOUT,
   /**
    * Forces LOCAL mode operation, even if the cache is configured to use a clustered mode like replication,
    * invalidation or distribution. Applying this flag will suppress any RPC messages otherwise associated with this
    * invocation. Write operations mat not acquire the entry locks. In distributed mode, the modifications performed
    * during an operation in a non-owner node are going to L1, if it is enabled, otherwise the operation is a no-op in
    * that node.
    */
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
   SKIP_LOCKING,
   /**
    * Forces a write lock, even if the invocation is a read operation.  Useful when reading an entry to later update it
    * within the same transaction, and is analogous in behavior and use case to a <tt>select ... for update ... </tt>
    * SQL statement.
    */
   FORCE_WRITE_LOCK,
   /**
    * Skips checking whether a cache is in a receptive state, i.e. is {@link ComponentStatus#RUNNING}.  May break
    * operation in weird ways!
    *
    * @deprecated This flag is no longer in use.
    */
   @Deprecated
   SKIP_CACHE_STATUS_CHECK,
   /**
    * Forces asynchronous network calls where possible, even if otherwise configured to use synchronous network calls.
    * Only applicable to non-local, clustered caches.
    */
   FORCE_ASYNCHRONOUS,
   /**
    * Forces synchronous network calls where possible, even if otherwise configured to use asynchronous network calls.
    * Only applicable to non-local, clustered caches.
    */
   FORCE_SYNCHRONOUS,
   /**
    * Skips storing an entry to any configured {@link org.infinispan.persistence.spi.CacheLoader}s.
    */
   SKIP_CACHE_STORE,
   /**
    * Skips loading an entry from any configured {@link org.infinispan.persistence.spi.CacheLoader}s. Useful for example to perform a put() operation
    * while not interested in the return value of <tt>put()</tt> which would return the eventually existing previous value.
    * <br>
    * Note that if you want to ignore the return value of <tt>put()</tt> and you are in distributed mode
    * you should also use the {@link #SKIP_REMOTE_LOOKUP} flag.
    */
   SKIP_CACHE_LOAD,
   /**
    * <p>Swallows any exceptions, logging them instead at a low log level.  Will prevent a failing operation from
    * affecting any ongoing JTA transactions as well.</p>
    * <p>This Flag will not be replicated to remote nodes, but it will still protect the invoker from remote exceptions.</p>
    * <p>When using this flag with Optimistic caches, lock acquisition happen in the prepare phase at which
    * point this flag will be ignored in order to ensure that Infinispan reports the correct exception
    * back to the transaction manager. This is done for safety reasons to avoid inconsistent cache contents.</p>
    */
   FAIL_SILENTLY,
   /**
    * When used with <b>distributed</b> cache mode, will prevent retrieving a remote value either when executing a get()
    * or exists(), or to provide an overwritten return value for a put() or remove().  This would render return values
    * for some operations (such as {@link Cache#put(Object, Object)} or {@link Cache#remove(Object)} unusable, in
    * exchange for the performance gains of reducing remote calls.
    * <br>
    * Note that if you want to ignore the return value of <tt>put()</tt> and you have configured a cache store
    * you should also use the {@link #SKIP_CACHE_LOAD} flag.
    */
   SKIP_REMOTE_LOOKUP,

   /**
    * Used by the Query module only, it will prevent the indexes to be updated as a result of the current operations.
    */
   SKIP_INDEXING,

   /**
    * Flags the invocation as a {@link Cache#putForExternalRead(Object, Object)}
    * call, as opposed to a regular {@link Cache#put(Object, Object)}. This
    * flag was created purely for internal Infinispan usage, and should not be
    * used by clients calling into Infinispan.
    */
   PUT_FOR_EXTERNAL_READ,

   /**
    * Flags the invocation as a put operation done internally by the state transfer.
    * This flag was created purely for internal Infinispan usage, and should not be
    * used by clients calling into Infinispan.
    */
   PUT_FOR_STATE_TRANSFER,

   /**
    * Flags the invocation as a put operation done internally by the cross-site state transfer. This flag was created
    * purely for internal Infinispan usage, and should not be used by clients calling into Infinispan.
    */
   PUT_FOR_X_SITE_STATE_TRANSFER,

   /**
    * If this flag is enabled, if a cache store is shared, then storage to the store is skipped.
    */
   SKIP_SHARED_CACHE_STORE,
   /**
    * This flag has only effect when it's used before calling {@link
    * org.infinispan.Cache#stop()} and its effect is that apart from stopping
    * the cache, it removes all of its content from both memory and any backing
    * cache store.
    *
    * @deprecated No longer in use.
    */
   @Deprecated
   REMOVE_DATA_ON_STOP,
   /**
    * Used by the DistLockingInterceptor to commit the change no matter what (if the flag is set). This is used when
    * a node A pushes state to another node B and A doesn't want B to check if the state really belongs to it
    */
   SKIP_OWNERSHIP_CHECK,
   /**
    * Signals when a particular cache write operation is writing a delta of
    * the object, rather than the full object. This can be useful in order to
    * make decisions such as whether the cache store needs checking to see if
    * the previous value needs to be loaded and merged.
    */
   DELTA_WRITE,

   /**
    * Signals that an operation's return value will be ignored. Typical
    * operations whose return value might be ignored include
    * {@link java.util.Map#put(Object, Object)} whose return value indicates
    * previous value. So, a user might decide to the put something in the
    * cache but might not be interested in the return value. This flag is ignored
    * with the conditional remove ({@link java.util.concurrent.ConcurrentMap#remove(Object, Object)})
    * and conditional replace ({@link java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)})
    * operations which return a boolean result: this has a minimal serialization payload and is
    * generally relevant in order to determine whether the operation was successful or not.
    *
    * Not requiring return values makes the cache behave more efficiently by
    * applying flags such as {@link Flag#SKIP_REMOTE_LOOKUP} or
    * {@link Flag#SKIP_CACHE_LOAD}.
    */
   IGNORE_RETURN_VALUES,

   /**
    * If cross-site replication is enabled, this would skip the replication to any remote site.
    */
   SKIP_XSITE_BACKUP,

   /**
    * Using a synchronous cache (whether replicated or distributed) provides
    * the cache caller guarantees that data has been sent to other cluster
    * nodes correctly and has been applied successfully. At the network
    * level, message delivery acknowledgement protocols are used to provide
    * these guarantees.
    *
    * In order to increase performance and achieve better throughput, it's
    * common to use negative acknowledgment protocols to confirm the
    * delivery of messages. The problem with these protocols is that if the
    * last message is lost, it can be difficult to recover it because a new
    * message would need to be sent to find the gap.
    *
    * Some cache use cases might involve storing an entry in a synchronously
    * replicated or distributed cache, and if that store operation fails, the
    * application fails to start. One such example is the Infinispan Hot Rod
    * server. When it starts, the first thing it does is add its endpoint
    * information to a cache which is used to notify clients of topology
    * changes. If this operation fails, the server cannot start because
    * topologies won't include this node, and no more cache operations
    * are attempted.
    *
    * So, in exceptional use cases such as this, a cluster wide cache update
    * should be positively acknowledged by the other nodes, so that if the
    * data is lost, it can be retransmitted immediately without the need
    * to wait for an extra cluster wide operation to detect the lost message.
    * The way to force a particular cache operation to be positively
    * acknowledged is to send this {@link #GUARANTEED_DELIVERY} flag.
    *
    * Note that this is flag is <b>EXPERIMENTAL</b> and so there is a high
    * probability that it will be removed in future Infinispan versions.
    */
   GUARANTEED_DELIVERY,

   /**
    * This flag skips listener notifications as a result of a cache operation.
    * For example, if this flag is passed as a result of a {@link Cache#get(Object)}
    * call, no callbacks will be made on listeners annotated with
    * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited}.
    */
   SKIP_LISTENER_NOTIFICATION,

   /**
    * This flag skips statistics updates as a result of a cache operation.
    * For example, if this flag is passed as a result of a {@link Cache#get(Object)}
    * call, no cache hits or cache miss counters will be updated.
    */
   SKIP_STATISTICS,

   /**
    * Flag to identify cache operations coming from the Hot Rod server.
    */
   OPERATION_HOTROD,

   /**
    * Flag to identify cache operations coming from the Memcached server.
    */
   OPERATION_MEMCACHED,

   /**
    * Any time a new indexed entry is inserted, a delete statement is issued on the indexes
    * to remove previous values. This delete statement is executed even if there is no known
    * entry having the same key. Enable this flag when you know for sure there is no existing
    * entry in the index for improved performance.
    * For example, this is useful for speeding up import of new data in an empty cache
    * having an empty index.
    */
   SKIP_INDEX_CLEANUP,

   /**
    * If a write operation encounters a retry due to a topology change this flag should be used to tell the new owner
    * that such a thing happened.  This flag was created purely for internal Infinispan usage, and should not be
    * used by clients calling into Infinispan.
    */
   COMMAND_RETRY

   ;

   /**
    * Creates a copy of a Flag Set removing instances of FAIL_SILENTLY.
    * The copy might be the same instance if no change is required,
    * and should be considered immutable.
    * @param flags
    * @return might return the same instance
    */
   public static Set<Flag> copyWithoutRemotableFlags(Set<Flag> flags) {
      //FAIL_SILENTLY should not be sent to remote nodes
      if (flags != null && flags.contains(Flag.FAIL_SILENTLY)) {
         EnumSet<Flag> copy = EnumSet.copyOf(flags);
         copy.remove(Flag.FAIL_SILENTLY);
         if (copy.isEmpty()) {
            return InfinispanCollections.emptySet();
         }
         else {
            return copy;
         }
      } else {
         return flags;
      }
   }

   public static class Externalizer extends AbstractExternalizer<Flag> {

      @Override
      public Integer getId() {
         return Ids.FLAG;
      }

      @Override
      public Set<Class<? extends Flag>> getTypeClasses() {
         return Util.<Class<? extends Flag>>asSet(Flag.class);
      }

      @Override
      public void writeObject(ObjectOutput output, Flag flag) throws IOException {
         output.writeByte(flag.ordinal());
      }

      @Override
      public Flag readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte flagByte = input.readByte();
         try {
            // values() cached by Class.getEnumConstants()
            return Flag.values()[flagByte];
         } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalStateException("Unknown flag index: " + flagByte);
         }
      }

   }

}
