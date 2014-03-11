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

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.CacheStoreConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.loaders.decorators.AbstractDelegatingStore.undelegateCacheLoader;

@MBean(objectName = "CacheLoader", description = "Component that handles loading entries from a CacheStore into memory.")
public class CacheLoaderInterceptor extends JmxStatsCommandInterceptor {
   private final AtomicLong cacheLoads = new AtomicLong(0);
   private final AtomicLong cacheMisses = new AtomicLong(0);

   protected CacheLoaderManager clm;
   protected CacheNotifier notifier;
   protected CacheLoader loader;
   protected volatile boolean enabled = true;
   protected EntryFactory entryFactory;
   private DataContainer dataContainer;

   private static final Log log = LogFactory.getLog(CacheLoaderInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void injectDependencies(CacheLoaderManager clm, EntryFactory entryFactory, CacheNotifier notifier,
                                     DataContainer dataContainer) {
      this.clm = clm;
      this.notifier = notifier;
      this.entryFactory = entryFactory;
      this.dataContainer = dataContainer;
   }

   @Start(priority = 15)
   protected void startInterceptor() {
      loader = clm.getCacheLoader();
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeeded(ctx, key, false, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeeded(ctx, key, false, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }


   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeededAndUpdateStats(ctx, key, true, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      if (enabled) {
         Object[] keys;
         if ((keys = command.getKeys()) != null && keys.length > 0) {
            for (Object key : command.getKeys()) {
               loadIfNeeded(ctx, key, false, command);
            }
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeededAndUpdateStats(ctx, key, false, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeededAndUpdateStats(ctx, key, false, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   protected boolean forceLoad(Object key, Set<Flag> flags) {
      return false;
   }

   /**
    * Indicates whether the operation is a delta write. If it is, the
    * previous value needs to be loaded from the cache store so that
    * it can be merged.
    */
   protected boolean isDeltaWrite(Set<Flag> flags) {
      return flags != null && flags.contains(Flag.DELTA_WRITE);
   }

   private boolean shouldSkipCacheLoader(FlagAffectedCommand cmd) {
      return cmd.hasFlag(Flag.SKIP_CACHE_STORE) || cmd.hasFlag(Flag.SKIP_CACHE_LOAD);
   }

   protected boolean canLoad(Object key) {
      return true;
   }

   /**
    * Loads from the cache loader the entry for the given key.  A found value is loaded into the current context.  The
    * method returns whether the value was found or not, or even if the cache loader was checked.
    * @param ctx The current invocation's context
    * @param key The key for the entry to look up
    * @param isRetrieval Whether or not this was called in the scope of a get
    * @param cmd The command that was called that now wants to query the cache loader
    * @return Whether or not the entry was found in the cache loader.  A value of null means the cache loader was never
    *         queried for the value, so it was neither a hit or a miss.
    * @throws Throwable
    */
   protected final Boolean loadIfNeeded(final InvocationContext ctx, Object key, boolean isRetrieval, final FlagAffectedCommand cmd) throws Throwable {
      if (shouldSkipCacheLoader(cmd) || cmd.hasFlag(Flag.IGNORE_RETURN_VALUES) || !canLoad(key)) {
         return null; //skip operation
      }

      // If this is a remote call, skip loading UNLESS we are the primary data owner of this key, and
      // are using eviction or write skew checking.
      if (!isRetrieval && !ctx.isOriginLocal() && !forceLoad(key, cmd.getFlags())) return false;

      // first check if the container contains the key we need.  Try and load this into the context.
      CacheEntry e = ctx.lookupEntry(key);
      if (!shouldAttemptLookup(e)) {
         return null;
      }

      final boolean isDelta = cmd instanceof ApplyDeltaCommand;
      final AtomicReference<Boolean> isLoaded = new AtomicReference<Boolean>();
      dataContainer.compute(key, new DataContainer.ComputeAction() {
         @Override
         public InternalCacheEntry compute(Object key, InternalCacheEntry oldEntry,
                                           InternalEntryFactory factory) {
            //under the lock, check if the entry exists in the DataContainer
            if (oldEntry != null) {
               wrapInternalCacheEntry(ctx, key, cmd, oldEntry, isDelta);
               isLoaded.set(null); //not loaded
               return oldEntry; //no changes in container
            }

            InternalCacheEntry loaded;
            try {
               loaded = internalLoadAndUpdateStats(key);
            } catch (CacheLoaderException e1) {
               isLoaded.set(null);
               return null;
            }
            if(loaded == null) {
               isLoaded.set(Boolean.FALSE); //not loaded
               return null; //no changed in container
            }

            CacheEntry wrappedEntry = wrapInternalCacheEntry(ctx, key, cmd, loaded, isDelta);
            recordLoadedEntry(ctx, key, wrappedEntry, loaded);

            isLoaded.set(Boolean.TRUE); //loaded!
            return loaded;
         }
      });

      return isLoaded.get();
   }

   private InternalCacheEntry internalLoadAndUpdateStats(Object key) throws CacheLoaderException {
      final InternalCacheEntry loaded = loader.load(key);
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Loaded %s for key %s from persistence.", loaded, key);
      }
      if(loaded == null || loaded.isExpired()) {
         return null;
      }
      return loaded;
   }

   /**
    * Only perform if context doesn't have a value found (Read Committed) or if we can do a remote
    * get only if the value is null (Repeatable Read)
    */
   private boolean shouldAttemptLookup(CacheEntry e) {
      return e == null || (e.isNull() || e.getValue() == null);
   }

   private CacheEntry wrapInternalCacheEntry(InvocationContext ctx, Object key, FlagAffectedCommand cmd,
                                             InternalCacheEntry ice, boolean isDelta) {
      if (isDelta) {
         ctx.putLookedUpEntry(key, ice);
         return entryFactory.wrapEntryForDelta(ctx, key, ((ApplyDeltaCommand) cmd).getDelta());
      } else {
         MVCCEntry entry = entryFactory.wrapEntryForPut(ctx, key, ice, false, cmd);
         entry.setChanged(false);
         return entry;
      }
   }

   /**
    * This method records a loaded entry, performing the following steps: <ol> <li>Increments counters for reporting via
    * JMX</li> <li>updates the 'entry' reference (an entry in the current thread's InvocationContext) with the contents
    * of 'loadedEntry' (freshly loaded from the CacheStore) so that the loaded details will be flushed to the
    * DataContainer when the call returns (in the LockingInterceptor, when locks are released)</li> <li>Notifies
    * listeners</li> </ol>
    *
    * @param ctx         the current invocation's context
    * @param key         key to record
    * @param entry       the appropriately locked entry in the caller's context
    * @param loadedEntry the internal entry loaded from the cache store.
    */
   private void recordLoadedEntry(InvocationContext ctx, Object key,
         CacheEntry entry, InternalCacheEntry loadedEntry) {
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
   }

   protected void sendNotification(Object key, Object value, boolean pre, InvocationContext ctx) {
      notifier.notifyCacheEntryLoaded(key, value, pre, ctx);
   }

   private void loadIfNeededAndUpdateStats(InvocationContext ctx, Object key, boolean isRetrieval, FlagAffectedCommand cmd) throws Throwable {
      Boolean found = loadIfNeeded(ctx, key, isRetrieval, cmd);
      if (found != null && !found && getStatisticsEnabled()) {
         cacheMisses.incrementAndGet();
      }
   }

   @ManagedAttribute(
         description = "Number of entries loaded from cache store",
         displayName = "Number of cache store loads",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCacheLoaderLoads() {
      return cacheLoads.get();
   }

   @ManagedAttribute(
         description = "Number of entries that did not exist in cache store",
         displayName = "Number of cache store load misses",
         measurementType = MeasurementType.TRENDSUP
   )
   public long getCacheLoaderMisses() {
      return cacheMisses.get();
   }

   @Override
   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset Statistics"
   )
   public void resetStatistics() {
      cacheLoads.set(0);
      cacheMisses.set(0);
   }

   @ManagedAttribute(
         description = "Returns a collection of cache loader types which configured and enabled",
         displayName = "Returns a collection of cache loader types which configured and enabled",
         displayType = DisplayType.DETAIL)
   /**
    * This method returns a collection of cache loader types (fully qualified class names) that are configured and enabled.
    */
   public Collection<String> getCacheLoaders() {
      if (enabled && clm.isEnabled()) {
         if (loader instanceof ChainingCacheStore) {
            ChainingCacheStore chainingStore = (ChainingCacheStore) loader;
            LinkedHashMap<CacheStore, CacheStoreConfiguration> stores = chainingStore.getStores();
            Set<String> storeTypes = new HashSet<String>(stores.size());
            for (CacheStore cs : stores.keySet()) storeTypes.add(undelegateCacheLoader(cs).getClass().getName());
            return storeTypes;
         } else {
            return Collections.singleton(undelegateCacheLoader(loader).getClass().getName());
         }
      } else {
         return InfinispanCollections.emptySet();
      }
   }
   @ManagedOperation(
         description = "Disable all cache loaders of a given type, where type is a fully qualified class name of the cache loader to disable",
         displayName = "Disable all cache loaders of a given type"
   )
   /**
    * Disables a cache loader of a given type, where type is the fully qualified class name of a {@link CacheLoader} implementation.
    *
    * If the given type cannot be found, this is a no-op.  If more than one cache loader of the same type is configured,
    * all cache loaders of the given type are disabled.
    *
    * @param loaderType fully qualified class name of the cache loader type to disable
    */
   public void disableCacheLoader(@Parameter(name="loaderType", description="Fully qualified class name of a CacheLoader implementation") String loaderType) {
      if (enabled) clm.disableCacheStore(loaderType);
   }

   public void disableInterceptor() {
      enabled = false;
   }
}
