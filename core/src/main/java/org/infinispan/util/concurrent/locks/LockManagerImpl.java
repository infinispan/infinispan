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
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.concurrent.locks.containers.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Handles locks for the MVCC based LockingInterceptor
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@MBean(objectName = "LockManager", description = "Manager that handles MVCC locks for entries")
public class LockManagerImpl implements LockManager {
   protected Configuration configuration;
   protected LockContainer lockContainer;
   private TransactionManager transactionManager;
   private InvocationContextContainer invocationContextContainer;
   private static final Log log = LogFactory.getLog(LockManagerImpl.class);
   protected static final boolean trace = log.isTraceEnabled();
   private static final String ANOTHER_THREAD = "(another thread)";

   @Inject
   public void injectDependencies(Configuration configuration, TransactionManager transactionManager, InvocationContextContainer invocationContextContainer) {
      this.configuration = configuration;
      this.transactionManager = transactionManager;
      this.invocationContextContainer = invocationContextContainer;
   }

   @Start
   public void startLockManager() {
      lockContainer = configuration.isUseLockStriping() ?
      transactionManager == null ? new ReentrantStripedLockContainer(configuration.getConcurrencyLevel()) : new OwnableReentrantStripedLockContainer(configuration.getConcurrencyLevel(), invocationContextContainer) :
      transactionManager == null ? new ReentrantPerEntryLockContainer(configuration.getConcurrencyLevel()) : new OwnableReentrantPerEntryLockContainer(configuration.getConcurrencyLevel(), invocationContextContainer);
   }

   public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
      long lockTimeout = getLockAcquisitionTimeout(ctx);
      if (trace) log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);
      if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS) != null) {
         // successfully locked!
         if (ctx instanceof TxInvocationContext) {
            TxInvocationContext tctx = (TxInvocationContext) ctx;
            if (!tctx.isTransactionValid()) {
               Transaction tx = tctx.getTransaction();
               log.debugf("Successfully acquired lock, but the transaction %s is no longer valid!  Releasing lock.", tx);
               lockContainer.releaseLock(key);
               throw new IllegalStateException("Transaction "+tx+" appears to no longer be valid!");
            }
         }
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

   public void unlock(Object key) {
      if (trace) log.tracef("Attempting to unlock %s", key);
      lockContainer.releaseLock(key);
   }

   @SuppressWarnings("unchecked")
   public void unlock(InvocationContext ctx) {
      ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
      if (!entries.isEmpty()) {
         // unlocking needs to be done in reverse order.
         Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
         while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            if (possiblyLocked(entry)) {
               // has been locked!
               Object k = e.getKey();
               if (trace) log.tracef("Attempting to unlock %s", k);
               lockContainer.releaseLock(k);
            }
         }
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

   public void releaseLocks(InvocationContext ctx) {
      Object owner = ctx.getLockOwner();
      // clean up.
      // unlocking needs to be done in reverse order.
      ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
      Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
      if (trace) log.tracef("Number of entries in context: %s", entries.size());

      while (it.hasNext()) {
         Map.Entry<Object, CacheEntry> e = it.next();
         CacheEntry entry = e.getValue();
         Object key = e.getKey();
         boolean needToUnlock = possiblyLocked(entry);
         // could be null with read-committed
         if (entry != null && entry.isChanged()) entry.rollback();
         else {
            if (trace) log.tracef("Entry for key %s is null, not calling rollbackUpdate", key);
         }
         // and then unlock
         if (needToUnlock) {
            if (trace) log.tracef("Releasing lock on [%s] for owner %s", key, owner);
            unlock(key);
         }
      }
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
}
