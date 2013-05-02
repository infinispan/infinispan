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

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.TimeService;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
   private TimeService timeService;
   private boolean enabled;
   private String cacheName;

   @Inject
   public void initialize(@ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR)
         ScheduledExecutorService executor, Cache cache, Configuration cfg, DataContainer dataContainer,
         CacheLoaderManager cacheLoaderManager, CacheNotifier cacheNotifier, TimeService timeService) {
      initialize(executor, cache.getName(), cfg, dataContainer,
            cacheLoaderManager, cacheNotifier, timeService);
   }

   void initialize(ScheduledExecutorService executor,
            String cacheName, Configuration cfg, DataContainer dataContainer,
            CacheLoaderManager cacheLoaderManager, CacheNotifier cacheNotifier, TimeService timeService) {
      this.executor = executor;
      this.configuration = cfg;
      this.cacheName = cacheName;
      this.dataContainer = dataContainer;
      this.cacheLoaderManager = cacheLoaderManager;
      this.cacheNotifier = cacheNotifier;
      this.timeService = timeService;
   }


   @Start(priority = 55)
   // make sure this starts after the CacheLoaderManager
   public void start() {
      // first check if eviction is enabled!
      enabled = configuration.expiration().reaperEnabled();
      if (enabled) {
         if (cacheLoaderManager != null && cacheLoaderManager.isEnabled()) {
            cacheStore = cacheLoaderManager.getCacheStore();
         }
         // Set up the eviction timer task
         long expWakeUpInt = configuration.expiration().wakeUpInterval();
         if (expWakeUpInt <= 0) {
            log.notStartingEvictionThread();
         } else {
            evictionTask = executor.scheduleWithFixedDelay(new ScheduledTask(),
                  expWakeUpInt, expWakeUpInt, TimeUnit.MILLISECONDS);
         }
      }
   }

   @Override
   public void processEviction() {
      long start = 0;
      if (!Thread.currentThread().isInterrupted()) {
         try {
            if (trace) {
               log.trace("Purging data container of expired entries");
               start = timeService.time();
            }
            dataContainer.purgeExpired();
            if (trace) {
               log.tracef("Purging data container completed in %s",
                          Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
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
                  start = timeService.time();
               }
               cacheStore.purgeExpired();
               if (trace) {
                  log.tracef("Purging cache store completed in %s",
                             Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
               }
            } catch (Exception e) {
               log.exceptionPurgingDataContainer(e);
            }
         }
      }
   }

   @Override
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
      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            processEviction();
         } finally {
            LogFactory.popNDC(trace);
         }
      }
   }

   @Override
   public void onEntryEviction(Map<Object, InternalCacheEntry> evicted) {
      // don't reuse the threadlocal context as we don't want to include eviction
      // operations in any ongoing transaction, nor be affected by flags
      // especially see ISPN-1154: it's illegal to acquire locks in a committing transaction
      InvocationContext ctx = ImmutableContext.INSTANCE;
      // This is important because we make no external guarantees on the thread
      // that will execute this code, so it could be the user thread, or could
      // be the eviction thread.
      // However, when a user calls cache.evict(), you do want to carry over the
      // contextual information, hence it makes sense for the notifyCacheEntriesEvicted()
      // call to carry on taking an InvocationContext object.
      cacheNotifier.notifyCacheEntriesEvicted(evicted.values(), ctx, null);
   }
}
