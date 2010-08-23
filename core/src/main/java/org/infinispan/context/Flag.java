package org.infinispan.context;

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
 *    <li>{@link #CACHE_MODE_LOCAL} - forces LOCAL mode opersation, even if the cache is configured to use a clustered
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
    * Forces LOCAL mode opersation, even if the cache is configured to use a clustered mode like replication,
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
    * Swallows any exceptions, logging them instead at a low log level.  Will prevent a failing operation from
    * affecting any ongoing JTA transactions as well.
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
   PUT_FOR_EXTERNAL_READ
}
