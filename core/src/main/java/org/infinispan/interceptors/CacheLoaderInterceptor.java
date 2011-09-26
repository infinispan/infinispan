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
package org.infinispan.interceptors;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.concurrent.atomic.AtomicLong;

@MBean(objectName = "CacheLoader", description = "Component that handles loading entries from a CacheStore into memory.")
public class CacheLoaderInterceptor extends JmxStatsCommandInterceptor {
   private final AtomicLong cacheLoads = new AtomicLong(0);
   private final AtomicLong cacheMisses = new AtomicLong(0);

   protected CacheLoaderManager clm;
   protected CacheNotifier notifier;
   protected CacheLoader loader;
   private DataContainer dataContainer;
   private EntryFactory entryFactory;

   @Inject
   protected void injectDependencies(CacheLoaderManager clm, DataContainer dataContainer, EntryFactory entryFactory, CacheNotifier notifier) {
      this.clm = clm;
      this.dataContainer = dataContainer;
      this.notifier = notifier;
      this.entryFactory = entryFactory;
   }

   @Start(priority = 15)
   protected void startInterceptor() {
      loader = clm.getCacheLoader();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Object key;
      if ((key = command.getKey()) != null) {
         loadIfNeeded(ctx, key);
      }
      return invokeNextInterceptor(ctx, command);
   }


   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key;
      if ((key = command.getKey()) != null) {
         loadIfNeededAndUpdateStats(ctx, key);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      Object[] keys;
      if ((keys = command.getKeys()) != null && keys.length > 0) {
         for (Object key : command.getKeys()) {
            loadIfNeeded(ctx, key);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object key;
      if ((key = command.getKey()) != null) {
         loadIfNeededAndUpdateStats(ctx, key);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object key;
      if ((key = command.getKey()) != null) {
         loadIfNeededAndUpdateStats(ctx, key);
      }
      return invokeNextInterceptor(ctx, command);
   }

   private boolean loadIfNeeded(InvocationContext ctx, Object key) throws Throwable {
      if (ctx.hasFlag(Flag.SKIP_CACHE_STORE) || ctx.hasFlag(Flag.SKIP_CACHE_LOAD)) {
         return false; //skip operation
      }
      // first check if the container contains the key we need.  Try and load this into the context.
      CacheEntry e = entryFactory.wrapEntryForReading(ctx, key);
      if (e == null || e.isNull()) {

         // Obtain a temporary lock to verify the key is not being concurrently added
         boolean keyLocked = entryFactory.acquireLock(ctx, key);
         boolean unlockOnWayOut = false;
         try {
            // check again, in case there is a concurrent addition
            if (dataContainer.containsKey(key)) {
               log.trace("No need to load.  Key exists in the data container.");
               unlockOnWayOut = true;
               return true;
            }
         } finally {
            if (keyLocked && unlockOnWayOut) {
               entryFactory.releaseLock(ctx, key);
            }
         }

         // we *may* need to load this.
         InternalCacheEntry loaded = loader.load(key);
         if (loaded == null) {
            if (log.isTraceEnabled()) {
               log.trace("No need to load.  Key doesn't exist in the loader.");
            }
            if (keyLocked) {
               entryFactory.releaseLock(ctx, key);
            }
            return false;
         }

         // Reuse the lock and create a new entry for loading
         MVCCEntry n = entryFactory.wrapEntryForWriting(ctx, key, true, false, keyLocked, false, true);
         recordLoadedEntry(ctx, key, n, loaded);
         return true;
      } else {
         return true;
      }
   }

   /**
    * This method records a loaded entry, performing the following steps:
    * <ol>
    * <li>Increments counters for reporting via JMX</li>
    * <li>updates the 'entry' reference (an entry in the current thread's InvocationContext) with the contents of
    * 'loadedEntry' (freshly loaded from the CacheStore) so that the loaded details will be flushed to the DataContainer
    * when the call returns (in the LockingInterceptor, when locks are released)</li>
    * <li>Notifies listeners</li>
    * </ol>
    * @param ctx the current invocation's context
    * @param key key to record
    * @param entry the appropriately locked entry in the caller's context
    * @param loadedEntry the internal entry loaded from the cache store.
    */
   private MVCCEntry recordLoadedEntry(InvocationContext ctx, Object key, MVCCEntry entry, InternalCacheEntry loadedEntry) throws Exception {
      boolean entryExists = loadedEntry != null;
      if (log.isTraceEnabled()) {
         log.trace("Entry exists in loader? " + entryExists);
      }

      if (getStatisticsEnabled()) {
         if (entryExists) {
            cacheLoads.incrementAndGet();
         } else {
            cacheMisses.incrementAndGet();
         }
      }

      if (entryExists) {
         final Object value = loadedEntry.getValue();
         // FIXME: There's no point to trigger the entryLoaded/Activated event twice.
         sendNotification(key, value, true, ctx);
         entry.setValue(value);
         entry.setLifespan(loadedEntry.getLifespan());
         entry.setMaxIdle(loadedEntry.getMaxIdle());
         // TODO shouldn't we also be setting last used and created timestamps?
         entry.setValid(true);

         sendNotification(key, value, false, ctx);
      }

      return entry;
   }

   protected void sendNotification(Object key, Object value, boolean pre, InvocationContext ctx) {
      notifier.notifyCacheEntryLoaded(key, value, pre, ctx);
   }

   private void loadIfNeededAndUpdateStats(InvocationContext ctx, Object key) throws Throwable {
      boolean found = loadIfNeeded(ctx, key);
      if (!found && getStatisticsEnabled()) {
         cacheMisses.incrementAndGet();
      }
   }

   @ManagedAttribute(description = "Number of entries loaded from cache store")
   @Metric(displayName = "Number of cache store loads", measurementType = MeasurementType.TRENDSUP)
   public long getCacheLoaderLoads() {
      return cacheLoads.get();
   }

   @ManagedAttribute(description = "Number of entries that did not exist in cache store")
   @Metric(displayName = "Number of cache store load misses", measurementType = MeasurementType.TRENDSUP)
   public long getCacheLoaderMisses() {
      return cacheMisses.get();
   }

   @Override
   @ManagedOperation(description = "Resets statistics gathered by this component")
   @Operation(displayName = "Reset Statistics")
   public void resetStatistics() {
      cacheLoads.set(0);
      cacheMisses.set(0);
   }
}