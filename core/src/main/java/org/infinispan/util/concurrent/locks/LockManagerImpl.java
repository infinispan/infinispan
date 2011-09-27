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

import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.containers.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
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
   protected volatile LockContainer lockContainer;
   private TransactionManager transactionManager;
   private static final Log log = LogFactory.getLog(LockManagerImpl.class);
   protected static final boolean trace = log.isTraceEnabled();
   private static final String ANOTHER_THREAD = "(another thread)";

   @Inject
   public void injectDependencies(Configuration configuration, TransactionManager transactionManager) {
      this.configuration = configuration;
      this.transactionManager = transactionManager;
   }

   @Start (priority = 8)
   public void startLockManager() {
      lockContainer = configuration.isUseLockStriping() ?
      transactionManager == null ? new ReentrantStripedLockContainer(configuration.getConcurrencyLevel()) : new OwnableReentrantStripedLockContainer(configuration.getConcurrencyLevel()) :
      transactionManager == null ? new ReentrantPerEntryLockContainer(configuration.getConcurrencyLevel()) : new OwnableReentrantPerEntryLockContainer(configuration.getConcurrencyLevel());
   }

   public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
      long lockTimeout = getLockAcquisitionTimeout(ctx);
      if (trace) log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);
      if (lockContainer.acquireLock(ctx, key, lockTimeout, MILLISECONDS) != null) {
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

   protected long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   public void unlock(InvocationContext ctx, Object key) {
      if (trace) log.tracef("Attempting to unlock %s", key);
      lockContainer.releaseLock(ctx, key);
   }

   @SuppressWarnings("unchecked")
   public void unlockAll(InvocationContext ctx) {
      for (Object k : ctx.getLockedKeys()) {
         if (trace) log.tracef("Attempting to unlock %s", k);
         lockContainer.releaseLock(ctx, k);
      }
   }

   public boolean ownsLock(Object key, Object owner) {
      return lockContainer.ownsLock(key, owner);
   }

   public boolean isLocked(Object key) {
      return lockContainer.isLocked(key);
   }

   public Object getOwner(Object key) {
      if (lockContainer.isLocked(key)) {
         Lock l = lockContainer.getLock(key);

         if (l instanceof OwnableReentrantLock) {
            return ((OwnableReentrantLock) l).getOwner();
         } else {
            // cannot determine owner, JDK Reentrant locks only provide best-effort guesses.
            return ANOTHER_THREAD;
         }
      } else return null;
   }

   public String printLockInfo() {
      return lockContainer.toString();
   }

   public final boolean possiblyLocked(CacheEntry entry) {
      return entry == null || entry.isChanged() || entry.isNull() || entry.isLockPlaceholder();
   }

   @ManagedAttribute(description = "The concurrency level that the MVCC Lock Manager has been configured with.")
   @Metric(displayName = "Concurrency level", dataType = DataType.TRAIT)
   public int getConcurrencyLevel() {
      return configuration.getConcurrencyLevel();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are held.")
   @Metric(displayName = "Number of locks held")
   public int getNumberOfLocksHeld() {
      return lockContainer.getNumLocksHeld();
   }

   @ManagedAttribute(description = "The number of exclusive locks that are available.")
   @Metric(displayName = "Number of locks available")
   public int getNumberOfLocksAvailable() {
      return lockContainer.size() - lockContainer.getNumLocksHeld();
   }

   public int getLockId(Object key) {
      return lockContainer.getLockId(key);
   }

   public final boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      // don't EVER use lockManager.isLocked() since with lock striping it may be the case that we hold the relevant
      // lock which may be shared with another key that we have a lock for already.
      // nothing wrong, just means that we fail to record the lock.  And that is a problem.
      // Better to check our records and lock again if necessary.

      if (!ctx.hasLockedKey(key) && !ctx.hasFlag(Flag.SKIP_LOCKING)) {
         return lock(ctx, key);
      } else {
         logLockNotAcquired(ctx);
      }
      return false;
   }

   public final boolean acquireLockNoCheck(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      if (!ctx.hasFlag(Flag.SKIP_LOCKING)) {
         return lock(ctx, key);
      } else {
         logLockNotAcquired(ctx);
      }
      return false;
   }

   private boolean lock(InvocationContext ctx, Object key) throws InterruptedException {
      if (lockAndRecord(key, ctx)) {
         ctx.registerLockedKey(key);
         return true;
      } else {
         Object owner = getOwner(key);
         // if lock cannot be acquired, expose the key itself, not the marshalled value
         if (key instanceof MarshalledValue) {
            key = ((MarshalledValue) key).get();
         }
         throw new TimeoutException("Unable to acquire lock after [" + Util.prettyPrintTime(getLockAcquisitionTimeout(ctx)) + "] on key [" + key + "] for requestor [" +
               ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
      }
   }

   private void logLockNotAcquired(InvocationContext ctx) {
      if (trace) {
         if (ctx.hasFlag(Flag.SKIP_LOCKING))
            log.trace("SKIP_LOCKING flag used!");
         else
            log.trace("Already own lock for entry");
      }
   }
}
