package org.infinispan.context;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.AdvancedCache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.loaders.CacheStore;

/**
 * Available flags, which may be set on a per-invocation basis.  These are provided using the {@link AdvancedCache}
 * interface, using some of the overloaded methods that allow passing in of a variable number of Flags.
 * <p/>
 * <ul>
 *    <li>{@link #ZERO_LOCK_ACQUISITION_TIMEOUT} - overrides the {@link Configuration#setLockAcquisitionTimeout(long)}
 *                                                 configuration setting by ensuring lock managers use a 0-millisecond
 *                                                 lock acquisition timeout.  Useful if you only want to acquire a lock
 *                                                 on an entry <i>if and only if</i> the lock is uncontended.</li>
 *    <li>{@link #CACHE_MODE_LOCAL} - forces LOCAL mode operation, even if the cache is configured to use a clustered
 *                                    mode like replication, invalidation or distribution.  Applying this flag will
 *                                    suppress any RPC messages otherwise associated with this invocation.</li>
 *    <li>{@link #SKIP_LOCKING} - bypasses lock acquisition for this invocation altogether.  A potentially dangerous
 *                                flag, as it can lead to inconsistent data.</li>
 *    <li>{@link #FORCE_WRITE_LOCK} - forces a write lock, even if the invocation is a read operation.  Useful when
 *                                    reading an entry to later update it within the same transaction, and is analogous
 *                                    in behavior and use case to a <tt>select ... for update ... </tt> SQL statement.</li>
 *    <li>{@link #SKIP_CACHE_STATUS_CHECK} - skips checking whether a cache is in a receptive state, i.e. is
 *                                          {@link ComponentStatus#RUNNING}.  May break operation in weird ways!</li>
 *    <li>{@link #FORCE_ASYNCHRONOUS} - forces asynchronous network calls where possible, even if otherwise configured
 *                                      to use synchronous network calls.  Only applicable to non-local, clustered caches.</li>
 *    <li>{@link #FORCE_SYNCHRONOUS} - forces synchronous network calls where possible, even if otherwise configured
 *                                      to use asynchronous network calls.  Only applicable to non-local, clustered caches.</li>
 *    <li>{@link #SKIP_CACHE_STORE} - skips storing an entry to any configured {@link CacheStore}s.</li>
 *    <li>{@link #FAIL_SILENTLY} - swallows any exceptions, logging them instead at a low log level.  Will prevent a
 *                                 failing operation from affecting any ongoing JTA transactions as well.</li>
 *    <li>{@link #SKIP_REMOTE_LOOKUP} - when used with <b>distributed</b> cache mode, will prevent retrieving a remote
 *                                      value either when executing a get() or exists(), or to provide an overwritten
 *                                      return value for a put() or remove().  This would render return values for some
 *                                      operations (such as {@link Cache#put(Object, Object)} or {@link Cache#remove(Object)}
 *                                      unusable, in exchange for the performance gains of reducing remote calls.</li>
 *    <li> {@link #PUT_FOR_EXTERNAL_READ} - flags the invocation as a {@link Cache#putForExternalRead(Object, Object)}
 *                                          call, as opposed to a regular {@link Cache#put(Object, Object)}.</li>
 * </ul>
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
    * invalidation or distribution.  Applying this flag will suppress any RPC messages otherwise associated with this
    * invocation.
    */
   CACHE_MODE_LOCAL,
   /**
    * Bypasses lock acquisition for this invocation altogether.  A potentially dangerous flag, as it can lead to
    * inconsistent data.
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
    */
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
    * Skips storing an entry to any configured {@link CacheStore}s.
    */
   SKIP_CACHE_STORE,
   /**
    * Skips loading an entry from any configured {@link CacheStore}s. Useful for example to perform a put() operation
    * while not interested in the return value of put() which would return the eventually existing previous value.
    */
   SKIP_CACHE_LOAD,
   /**
    * <p>Swallows any exceptions, logging them instead at a low log level.  Will prevent a failing operation from
    * affecting any ongoing JTA transactions as well.</p>
    * <p>This Flag will not be replicated to remote nodes, but it will still protect the invoker from remote exceptions.</p>
    */
   FAIL_SILENTLY,
   /**
    * When used with <b>distributed</b> cache mode, will prevent retrieving a remote value either when executing a get()
    * or exists(), or to provide an overwritten return value for a put() or remove().  This would render return values
    * for some operations (such as {@link Cache#put(Object, Object)} or {@link Cache#remove(Object)} unusable, in
    * exchange for the performance gains of reducing remote calls.
    */
   SKIP_REMOTE_LOOKUP,

   /**
    * Flags the invocation as a {@link Cache#putForExternalRead(Object, Object)} call, as opposed to a regular
    * {@link Cache#put(Object, Object)}.
    */
   PUT_FOR_EXTERNAL_READ,
   /**
    * If this flag is enabled, if a cache store is shared, then storage to the store is skipped.
    */
   SKIP_SHARED_CACHE_STORE;
   
   /**
    * Creates a copy of a Flag Set removing instances of FAIL_SILENTLY.
    * The copy might be the same instance if no change is required,
    * and should be considered immutable.
    * @param flags
    * @return might return the same instance
    */
   protected static Set<Flag> copyWithouthRemotableFlags(Set<Flag> flags) {
      //FAIL_SILENTLY should not be sent to remote nodes
      if (flags.contains(Flag.FAIL_SILENTLY)) {
         EnumSet<Flag> copy = EnumSet.copyOf(flags);
         copy.remove(Flag.FAIL_SILENTLY);
         if (copy.isEmpty()) {
            return Collections.emptySet();
         }
         else {
            return copy;
         }
      } else {
         return flags;
      }
   }
}
