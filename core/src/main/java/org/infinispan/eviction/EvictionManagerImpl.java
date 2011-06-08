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
package org.infinispan.eviction;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@ThreadSafe
public class EvictionManagerImpl implements EvictionManager {
   private static final Log log = LogFactory.getLog(EvictionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   ScheduledFuture <?> evictionTask;

   // components to be injected
   private ScheduledExecutorService executor;
   private Configuration configuration;
   private CacheLoaderManager cacheLoaderManager;
   private DataContainer dataContainer;
   private CacheStore cacheStore;
   private CacheNotifier cacheNotifier;
   private LockManager lockManager;
   private PassivationManager passivator;
   private InvocationContextContainer ctxContainer;
   private boolean enabled;

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR) ScheduledExecutorService executor,
            Configuration configuration, DataContainer dataContainer,
            CacheLoaderManager cacheLoaderManager, CacheNotifier cacheNotifier,
            LockManager lockManager, PassivationManager passivator, InvocationContextContainer ctxContainer) {
      this.executor = executor;
      this.configuration = configuration;
      this.dataContainer = dataContainer;
      this.cacheLoaderManager = cacheLoaderManager;
      this.cacheNotifier = cacheNotifier;
      this.lockManager = lockManager;
      this.passivator = passivator;
      this.ctxContainer = ctxContainer;
   }

   @Start(priority = 55)
   // make sure this starts after the CacheLoaderManager
   public void start() {
      // first check if eviction is enabled!
      enabled = configuration.getEvictionStrategy() != EvictionStrategy.NONE;
      if (enabled) {
         if (cacheLoaderManager != null && cacheLoaderManager.isEnabled()) {
            cacheStore = cacheLoaderManager.getCacheStore();
         }
         // Set up the eviction timer task
         if (configuration.getEvictionWakeUpInterval() <= 0) {
            log.notStartingEvictionThread();
         } else {
            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(), configuration.getEvictionWakeUpInterval(),
                                                           configuration.getEvictionWakeUpInterval(), TimeUnit.MILLISECONDS);
         }
      }
   }

   public void processEviction() {
      long start = 0;
      if (!Thread.currentThread().isInterrupted()) {
         try {
            if (trace) {
               log.trace("Purging data container of expired entries");
               start = System.currentTimeMillis();
            }
            dataContainer.purgeExpired();
            if (trace) {
               log.tracef("Purging data container completed in %s", Util.prettyPrintTime(System.currentTimeMillis() - start));
            }
         } catch (Exception e) {
            log.exceptionPurgingDataContainer(e);
         }
      }

      if (!Thread.currentThread().isInterrupted()) {
         if (cacheStore != null) {
            try {
               if (trace) {
                  log.trace("Purging cache store of expired entries");
                  start = System.currentTimeMillis();
               }
               cacheStore.purgeExpired();
               if (trace) {
                  log.tracef("Purging cache store completed in %s", Util.prettyPrintTime(System.currentTimeMillis() - start));
               }
            } catch (Exception e) {
               log.exceptionPurgingDataContainer(e);
            }
         }
      }
   }

   public boolean isEnabled() {
      return enabled;
   }

   @Stop(priority = 5)
   public void stop() {
      if (evictionTask != null) {
         evictionTask.cancel(true);
      }
   }

   class ScheduledTask implements Runnable {
      public void run() {
         processEviction();
      }
   }

   @Override
   public void onEntryEviction(Map<Object, InternalCacheEntry> evicted) {
      // XXX: Note that this should be more efficient once ISPN-720 is resolved.
      for (Map.Entry<Object, InternalCacheEntry> e: evicted.entrySet()) {
         onEntryEviction(e.getKey(), e.getValue());
      }
   }

   private void onEntryEviction(Object key, InternalCacheEntry value) {
      final Object entryValue = value.getValue();
      InvocationContext context = getInvocationContext();

      cacheNotifier.notifyCacheEntryEvicted(key, entryValue, true, context);

      if (passivator.isEnabled()) {
         boolean locked = false;
         try {
            locked = acquireLock(context, key);
         } catch (Exception e) {
            log.couldNotAcquireLockForEviction(key, e);
         }
         try {
            passivator.passivate(key, value, null);
         } catch (CacheLoaderException e) {
            log.unableToPassivateEntry(key, e);
         }
         finally {
            if (locked) {
               releaseLock(key);
            }
         }
      }

      cacheNotifier.notifyCacheEntryEvicted(key, entryValue, false, getInvocationContext());
   }

   private InvocationContext getInvocationContext(){
      return ctxContainer.getInvocationContext();
   }

   /**
    * Attempts to lock an entry if the lock isn't already held in the current scope, and records the lock in the
    * context.
    *
    * @param ctx context
    * @param key Key to lock
    * @return true if a lock was needed and acquired, false if it didn't need to acquire the lock (i.e., lock was
    *         already held)
    * @throws InterruptedException if interrupted
    * @throws org.infinispan.util.concurrent.TimeoutException
    *                              if we are unable to acquire the lock after a specified timeout.
    */
   private boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
      // don't EVER use lockManager.isLocked() since with lock striping it may be the case that we hold the relevant
      // lock which may be shared with another key that we have a lock for already.
      // nothing wrong, just means that we fail to record the lock.  And that is a problem.
      // Better to check our records and lock again if necessary.

      boolean shouldSkipLocking = ctx.hasFlag(Flag.SKIP_LOCKING);

      if (!ctx.hasLockedKey(key) && !shouldSkipLocking) {
         if (lockManager.lockAndRecord(key, ctx)) {
            return true;
         } else {
            Object owner = lockManager.getOwner(key);
            // if lock cannot be acquired, expose the key itself, not the marshalled value
            if (key instanceof MarshalledValue) {
               key = ((MarshalledValue) key).get();
            }
            throw new TimeoutException("Unable to acquire lock after [" + Util.prettyPrintTime(getLockAcquisitionTimeout(ctx)) + "] on key [" + key + "] for requestor [" +
                  ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
         }
      } else {
         if (trace) {
            if (shouldSkipLocking) {
               log.trace("SKIP_LOCKING flag used!");
            } else {
               log.trace("Already own lock for entry");
            }
         }
      }

      return false;
   }

   public final void releaseLock(Object key) {
      lockManager.unlock(key);
   }

   private long getLockAcquisitionTimeout(InvocationContext ctx) {
      return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
            0 : configuration.getLockAcquisitionTimeout();
   }

}
