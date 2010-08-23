/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantStripedLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import javax.transaction.TransactionManager;
import java.util.Iterator;
import java.util.Map;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.concurrent.locks.Lock;

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
      if (trace) log.trace("Attempting to lock {0} with acquisition timeout of {1} millis", key, lockTimeout);
      if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS) != null) {
         // successfully locked!
         if (trace) log.trace("Successfully acquired lock!");         
         return true;
      }

      // couldn't acquire lock!
      return false;
   }

   protected long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   public void unlock(Object key) {
      if (trace) log.trace("Attempting to unlock " + key);
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
               if (trace) log.trace("Attempting to unlock " + k);
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
      return entry == null || entry.isChanged() || entry.isNull();
   }

   public void releaseLocks(InvocationContext ctx) {
      Object owner = ctx.getLockOwner();
      // clean up.
      // unlocking needs to be done in reverse order.
      ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
      Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
      if (trace) log.trace("Number of entries in context: {0}", entries.size());

      while (it.hasNext()) {
         Map.Entry<Object, CacheEntry> e = it.next();
         CacheEntry entry = e.getValue();
         Object key = e.getKey();
         boolean needToUnlock = possiblyLocked(entry);
         // could be null with read-committed
         if (entry != null && entry.isChanged()) entry.rollback();
         else {
            if (trace) log.trace("Entry for key {0} is null, not calling rollbackUpdate", key);
         }
         // and then unlock
         if (needToUnlock) {
            if (trace) log.trace("Releasing lock on [" + key + "] for owner " + owner);
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
}
