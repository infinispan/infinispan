/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.EntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;

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
      if ((key = command.getKey()) != null) loadIfNeeded(ctx, key);
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
      if ((keys = command.getKeys()) != null && keys.length > 0)
         for (Object key : command.getKeys()) loadIfNeeded(ctx, key);
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      Object key;
      if ((key = command.getKey()) != null) loadIfNeededAndUpdateStats(ctx, key);
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      Object key;
      if ((key = command.getKey()) != null) loadIfNeededAndUpdateStats(ctx, key);
      return invokeNextInterceptor(ctx, command);
   }

   private boolean loadIfNeeded(InvocationContext ctx, Object key) throws Throwable {
      // first check if the container contains the key we need.  Try and load this into the context.
      CacheEntry e = entryFactory.wrapEntryForReading(ctx, key);
      if (e == null || e.isNull()) {

         // we *may* need to load this.
         if (!loader.containsKey(key)) {
            if (log.isTraceEnabled()) log.trace("No need to load.  Key doesn't exist in the loader.");
            return false;
         }

         // Obtain a temporary lock to verify the key is not being concurrently added
         boolean keyLocked = entryFactory.acquireLock(ctx, key);

         // check again, in case there is a concurrent addition
         if (dataContainer.containsKey(key)) {
            if (keyLocked) entryFactory.releaseLock(key);
            log.trace("No need to load.  Key exists in the data container.");
            return true;
         }

         // Reuse the lock and create a new entry for loading
         MVCCEntry n = entryFactory.wrapEntryForWriting(ctx, key, true, false, keyLocked, false);
         n = loadEntry(ctx, key, n);
         return true;
      } else {
         return true;
      }
   }

   /**
    * Loads an entry from loader
    */
   private MVCCEntry loadEntry(InvocationContext ctx, Object key, MVCCEntry entry) throws Exception {
      log.trace("Loading key {0}", key);

      InternalCacheEntry storedEntry = loader.load(key);
      boolean entryExists = (storedEntry != null);
      if (log.isTraceEnabled()) log.trace("Entry exists in loader? " + entryExists);

      if (getStatisticsEnabled()) {
         if (entryExists) {
            cacheLoads.incrementAndGet();
         } else {
            cacheMisses.incrementAndGet();
         }
      }

      if (entryExists) {
         sendNotification(key, true, ctx);
         entry.setValue(storedEntry.getValue());
         entry.setLifespan(storedEntry.getLifespan());
         entry.setValid(true);

         notifier.notifyCacheEntryLoaded(key, false, ctx);
         sendNotification(key, false, ctx);
      }

      return entry;
   }

   protected void sendNotification(Object key, boolean pre, InvocationContext ctx) {
      notifier.notifyCacheEntryLoaded(key, pre, ctx);
   }

   private void loadIfNeededAndUpdateStats(InvocationContext ctx, Object key) throws Throwable {
      boolean found = loadIfNeeded(ctx, key);
      if (!found && getStatisticsEnabled()) {
         cacheMisses.incrementAndGet();
      }
   }

   @ManagedAttribute(description = "Number of CacheLoader loads")
   public long getCacheLoaderLoads() {
      return cacheLoads.get();
   }

   @ManagedAttribute(description = "Number of CacheLoader misses")
   public long getCacheLoaderMisses() {
      return cacheMisses.get();
   }

   @ManagedOperation(description = "Resets statistics gathered by this component")
   public void resetStatistics() {
      cacheLoads.set(0);
      cacheMisses.set(0);
   }
}