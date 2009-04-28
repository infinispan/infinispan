package org.infinispan.invocation;

/**
 * Available flags, which may be set on a per-invocation basis.  These are provided using the {@link
 * org.infinispan.AdvancedCache} interface, using some of the overloaded methods that allow passing in of a variable
 * number of Flags.
 * <p/>
 * <ul> <li>{@link #ZERO_LOCK_ACQUISITION_TIMEOUT} - overrides the {@link org.infinispan.config.Configuration#setLockAcquisitionTimeout(long)}
 * configuration setting by ensuring lock managers use a 0 lock acquisition timeout.</li> <li>{@link #CACHE_MODE_LOCAL}
 * - forces local mode even if the cache is configured to use a clustered mode like replication, invalidation or
 * distribution</li> <li>{@link #SKIP_LOCKING} - bypasses lock acquisition altogether</li> <li>{@link #FORCE_WRITE_LOCK}
 * - forces a write lock, even if the call is a read.  Useful when reading an entry to later update it within the same
 * transaction</li> <li>{@link #SKIP_CACHE_STATUS_CHECK} - skips checking whether a cache is in a receptive state, i.e.
 * is {@link org.infinispan.lifecycle.ComponentStatus#RUNNING}.  May break operation in weird ways!</li> <li>{@link
 * #FORCE_ASYNCHRONOUS} - forces asynchronous network calls where possible</li> <li>{@link #FORCE_SYNCHRONOUS} - forces
 * synchronous network calls where possible</li> <li>{@link #SKIP_CACHE_STORE} - skips storing an entry to any
 * configured {@link org.infinispan.loader.CacheStore}s</li> <li>{@link #FAIL_SILENTLY} - swallows any exceptions,
 * logging them instead at a low log level</li> <li>{@link #UNSAFE_UNRELIABLE_RETURN_VALUES} - when used with DIST cache
 * mode, will skip retrieving a remote value before overwriting it, rendering return values for some operations (such as
 * {@link org.infinispan.Cache#put(Object, Object)} or {@link org.infinispan.Cache#remove(Object)} unusable.</li> </ul>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public enum Flag {
   ZERO_LOCK_ACQUISITION_TIMEOUT,
   CACHE_MODE_LOCAL,
   SKIP_LOCKING,
   FORCE_WRITE_LOCK,
   SKIP_CACHE_STATUS_CHECK,
   FORCE_ASYNCHRONOUS,
   FORCE_SYNCHRONOUS,
   SKIP_CACHE_STORE,
   FAIL_SILENTLY,
   UNSAFE_UNRELIABLE_RETURN_VALUES
}
