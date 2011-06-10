/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PassivationManagerImpl implements PassivationManager {

   CacheLoaderManager cacheLoaderManager;
   CacheNotifier notifier;
   CacheStore cacheStore;
   Configuration cfg;

   boolean statsEnabled = false;
   boolean enabled = false;
   private static final Log log = LogFactory.getLog(PassivationManagerImpl.class);
   private final AtomicLong passivations = new AtomicLong(0);
   private DataContainer container;
   private static final boolean trace = log.isTraceEnabled();
   private LockManager lockManager;

   @Inject
   public void inject(CacheLoaderManager cacheLoaderManager, CacheNotifier notifier, Configuration cfg, DataContainer container, LockManager lockManager) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.notifier = notifier;
      this.cfg = cfg;
      this.container = container;
      this.lockManager = lockManager;
   }

   @Start(priority = 11)
   public void start() {
      enabled = cfg.getCacheLoaderManagerConfig().isPassivation();
      if (enabled) {
         cacheStore = cacheLoaderManager == null ? null : cacheLoaderManager.getCacheStore();
         if (cacheStore == null) {
            throw new ConfigurationException("passivation can only be used with a CacheLoader that implements CacheStore!");
         }

         enabled = cacheLoaderManager.isEnabled() && cacheLoaderManager.isUsingPassivation();
         statsEnabled = cfg.isExposeJmxStatistics();
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public void passivate(Map<Object, InternalCacheEntry> entries,
                         Map<Object, Object> nakedEntries, InvocationContext ctx) {
      if (enabled) {
         notifier.notifyCacheEntriesPassivated(nakedEntries, true, ctx);
         for (Map.Entry<Object, InternalCacheEntry> entry : entries.entrySet()) {
            Object key = entry.getKey();
            boolean locked = false;
            try {
               locked = acquireLock(ctx, key);
            } catch (Exception e) {
               log.couldNotAcquireLockForEviction(key, e);
            }
            try {
               // notify listeners that this entry is about to be passivated
               if (trace) log.tracef("Passivating entry %s", key);
               cacheStore.store(entry.getValue());
               if (statsEnabled && entry.getValue() != null) {
                  passivations.getAndIncrement();
               }
            } catch (CacheLoaderException e) {
               log.unableToPassivateEntry(key, e);
            }
            finally {
               if (locked) {
                  lockManager.unlock(key);
               }
            }
         }
         notifier.notifyCacheEntriesPassivated(nakedEntries, false, ctx);
      }
   }

   @Stop(priority = 9)
   public void passivateAll() throws CacheLoaderException {
      if (enabled) {
         long start = System.currentTimeMillis();
         log.passivatingAllEntries();
         for (InternalCacheEntry e : container) {
            if (trace) log.tracef("Passivating %s", e.getKey());
            cacheStore.store(e);
         }
         log.passivatedEntries(container.size(), Util.prettyPrintTime(System.currentTimeMillis() - start));
      }
   }

   public long getPassivationCount() {
      return passivations.get();
   }

   public void resetPassivationCount() {
      passivations.set(0L);
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

      if (lockManager.lockAndRecord(key, ctx)) {
         return true;
      } else {
         Object owner = lockManager.getOwner(key);
         // if lock cannot be acquired, expose the key itself, not the marshalled value
         if (key instanceof MarshalledValue) {
            key = ((MarshalledValue) key).get();
         }
         throw new TimeoutException(String.format(
            "Unable to acquire lock after [%d] on key [%s] for requestor [%s]! Lock held by [%s]",
            cfg.getLockAcquisitionTimeout(), key, ctx.getLockOwner(), owner));
      }
   }
}
