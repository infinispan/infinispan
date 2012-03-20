/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
    * Flags the invocation as a {@link Cache#putForExternalRead(Object, Object)} call, as opposed to a regular
    * {@link Cache#put(Object, Object)}.
    */
   PUT_FOR_EXTERNAL_READ,
   /**
    * If this flag is enabled, if a cache store is shared, then storage to the store is skipped.
    */
   SKIP_SHARED_CACHE_STORE,
   /**
    * This flag has only effect when it's used before calling {@link
    * org.infinispan.Cache#stop()} and its effect is that apart from stopping
    * the cache, it removes all of its content from both memory and any backing
    * cache store.
    */
   REMOVE_DATA_ON_STOP,
   /**
    * Used by the DistLockingInterceptor to commit the change no matter what (if the flag is set). This is used when
    * a node A pushes state to another node B and A doesn't want B to check if the state really belongs to it
    */
   SKIP_OWNERSHIP_CHECK,
   /**
    * indicates that the write skew check must be skipped. It happens when the commit or rollback command are received
    * before the prepare command (total order based scheme)
    */
   SKIP_WRITE_SKEW_CHECK;

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
