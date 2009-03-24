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
package org.horizon.lock;

import org.horizon.config.Configuration;
import org.horizon.container.MVCCEntry;
import org.horizon.context.InvocationContext;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.Start;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.invocation.Options;
import org.horizon.jmx.annotations.ManagedAttribute;
import org.horizon.jmx.annotations.MBean;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.util.ReversibleOrderedSet;
import org.horizon.util.concurrent.locks.OwnableReentrantLock;
import org.horizon.util.concurrent.locks.containers.LockContainer;
import org.horizon.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.horizon.util.concurrent.locks.containers.OwnableReentrantStripedLockContainer;
import org.horizon.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.horizon.util.concurrent.locks.containers.ReentrantStripedLockContainer;

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
@MBean(objectName = "MvccLockManager")
public class LockManagerImpl implements LockManager {
   protected Configuration configuration;
   LockContainer lockContainer;
   private TransactionManager transactionManager;
   private InvocationContextContainer invocationContextContainer;
   private static final Log log = LogFactory.getLog(LockManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

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

   public Object getLockOwner(InvocationContext ctx) {
      return ctx.getGlobalTransaction() != null ? ctx.getGlobalTransaction() : Thread.currentThread();
   }

   public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
      long lockTimeout = getLockAcquisitionTimeout(ctx);
      if (trace) log.trace("Attempting to lock {0} with acquisition timeout of {1} millis", key, lockTimeout);
      if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS)) {
         ctx.setContainsLocks(true);
         return true;
      }

      // couldn't acquire lock!
      return false;
   }

   private long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasOption(Options.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

   public void unlock(Object key, Object owner) {
      if (trace) log.trace("Attempting to unlock " + key);
      lockContainer.releaseLock(key);
   }

   @SuppressWarnings("unchecked")
   public void unlock(InvocationContext ctx) {
      ReversibleOrderedSet<Map.Entry<Object, MVCCEntry>> entries = ctx.getLookedUpEntries().entrySet();
      if (!entries.isEmpty()) {
         // unlocking needs to be done in reverse order.
         Iterator<Map.Entry<Object, MVCCEntry>> it = entries.reverseIterator();
         while (it.hasNext()) {
            Map.Entry<Object, MVCCEntry> e = it.next();
            MVCCEntry entry = e.getValue();
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
            // cannot determine owner.
            return null;
         }
      } else return null;
   }

   public String printLockInfo() {
      return lockContainer.toString();
   }

   public final boolean possiblyLocked(MVCCEntry entry) {
      return entry == null || entry.isChanged() || entry.isNullEntry();
   }

   @ManagedAttribute(writable = false, description = "The concurrency level that the MVCC Lock Manager has been configured with.")
   public int getConcurrencyLevel() {
      return configuration.getConcurrencyLevel();
   }

   @ManagedAttribute(writable = false, description = "The number of exclusive locks that are held.")
   public int getNumberOfLocksHeld() {
      return lockContainer.getNumLocksHeld();
   }

   @ManagedAttribute(writable = false, description = "The number of exclusive locks that are available.")
   public int getNumberOfLocksAvailable() {
      return lockContainer.size() - lockContainer.getNumLocksHeld();
   }
}
