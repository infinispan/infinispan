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
package org.infinispan.util.concurrent.locks;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.containers.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Handles locks for the MVCC based LockingInterceptor
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@MBean(objectName = "LockManager", description = "Manager that handles MVCC locks for entries")
public class LockManagerImpl implements LockManager {
   protected Configuration configuration;
   protected volatile LockContainer<?> lockContainer;
   private static final Log log = LogFactory.getLog(LockManagerImpl.class);
   protected static final boolean trace = log.isTraceEnabled();
   private static final String ANOTHER_THREAD = "(another thread)";

   @Inject
   public void injectDependencies(Configuration configuration, LockContainer<?> lockContainer) {
      this.configuration = configuration;
      this.lockContainer = lockContainer;
   }

   @Override
   public boolean lockAndRecord(Object key, InvocationContext ctx, long timeoutMillis) throws InterruptedException {
      if (trace) log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, timeoutMillis);
      if (lockContainer.acquireLock(ctx.getLockOwner(), key, timeoutMillis, MILLISECONDS) != null) {
         if (trace) log.tracef("Successfully acquired lock %s!", key);
         return true;
      }

      // couldn't acquire lock!
      if (log.isDebugEnabled()) {
         log.debugf("Failed to acquire lock %s, owner is %s", key, getOwner(key));
         Object owner = ctx.getLockOwner();
         Set<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
         List<Object> lockedKeys = new ArrayList<Object>(entries.size());
         for (Map.Entry<Object, CacheEntry> e : entries) {
            Object lockedKey = e.getKey();
            if (ownsLock(lockedKey, owner)) {
               lockedKeys.add(lockedKey);
            }
         }
         log.debugf("This transaction (%s) already owned locks %s", owner, lockedKeys);
      }
      return false;
   }

   @Override
   public void unlock(Collection<Object> lockedKeys, Object lockOwner) {
      log.tracef("Attempting to unlock keys %s", lockedKeys);
      for (Object k : lockedKeys) lockContainer.releaseLock(lockOwner, k);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void unlockAll(InvocationContext ctx) {
      for (Object k : ctx.getLockedKeys()) {
         if (trace) log.tracef("Attempting to unlock %s", k);
         lockContainer.releaseLock(ctx.getLockOwner(), k);
      }
      ctx.clearLockedKeys();
   }

   @Override
   public boolean ownsLock(Object key, Object owner) {
      return lockContainer.ownsLock(key, owner);
   }

   @Override
   public boolean isLocked(Object key) {
      return lockContainer.isLocked(key);
   }

   @Override
   public Object getOwner(Object key) {
      if (lockContainer.isLocked(key)) {
         Lock l = lockContainer.getLock(key);

         if (l instanceof OwnableReentrantLock) {
            return ((OwnableReentrantLock) l).getOwner();
         } else if (l instanceof VisibleOwnerReentrantLock) {
            Thread owner = ((VisibleOwnerReentrantLock) l).getOwner();
            // Don't assume the key is unlocked if getOwner() returned null.
            // JDK ReentrantLocks can return null e.g. if another thread is in the process of acquiring the lock
            if (owner != null)
               return owner;
         }

         return ANOTHER_THREAD;
      } else {
         // not locked
         return null;
      }
   }

   @Override
   public String printLockInfo() {
      return lockContainer.toString();
   }

   @Override
   public final boolean possiblyLocked(CacheEntry entry) {
      return entry == null || entry.isChanged() || entry.isNull();
   }

   @ManagedAttribute(description = "The concurrency level that the MVCC Lock Manager has been configured with.", displayName = "Concurrency level", dataType = DataType.TRAIT)
   public int getConcurrencyLevel() {
      return configuration.locking().concurrencyLevel();
   }

   @Override
   @ManagedAttribute(description = "The number of exclusive locks that are held.", displayName = "Number of locks held")
   public int getNumberOfLocksHeld() {
      return lockContainer.getNumLocksHeld();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are available.", displayName = "Number of locks available")
   public int getNumberOfLocksAvailable() {
      return lockContainer.size() - lockContainer.getNumLocksHeld();
   }

   @Override
   public int getLockId(Object key) {
      return lockContainer.getLockId(key);
   }

//   @Override
//   public final boolean acquireLock(InvocationContext ctx, Object key, boolean skipLocking) throws InterruptedException, TimeoutException {
//      return acquireLock(ctx, key, -1, skipLocking);
//   }

   @Override
   public boolean acquireLock(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      // don't EVER use lockManager.isLocked() since with lock striping it may be the case that we hold the relevant
      // lock which may be shared with another key that we have a lock for already.
      // nothing wrong, just means that we fail to record the lock.  And that is a problem.
      // Better to check our records and lock again if necessary.
      if (!ctx.hasLockedKey(key) && !skipLocking) {
//         return lock(ctx, key, timeoutMillis < 0 ? getLockAcquisitionTimeout(ctx) : timeoutMillis);
         return lock(ctx, key, timeoutMillis);
      } else {
         logLockNotAcquired(skipLocking);
      }
      return false;
   }

   @Override
   public final boolean acquireLockNoCheck(InvocationContext ctx, Object key, long timeoutMillis, boolean skipLocking) throws InterruptedException, TimeoutException {
      if (!skipLocking) {
         return lock(ctx, key, timeoutMillis);
      } else {
         logLockNotAcquired(skipLocking);
      }
      return false;
   }

   private boolean lock(InvocationContext ctx, Object key, long timeoutMillis) throws InterruptedException {
      if (lockAndRecord(key, ctx, timeoutMillis)) {
         ctx.addLockedKey(key);
         return true;
      } else {
         Object owner = getOwner(key);
         // if lock cannot be acquired, expose the key itself, not the marshalled value
         if (key instanceof MarshalledValue) {
            key = ((MarshalledValue) key).get();
         }
         throw new TimeoutException("Unable to acquire lock after [" + Util.prettyPrintTime(timeoutMillis) + "] on key [" + key + "] for requestor [" +
               ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
      }
   }

   private void logLockNotAcquired(boolean skipLocking) {
      if (trace) {
         if (skipLocking)
            log.trace("SKIP_LOCKING flag used!");
         else
            log.trace("Already own lock for entry");
      }
   }
}
