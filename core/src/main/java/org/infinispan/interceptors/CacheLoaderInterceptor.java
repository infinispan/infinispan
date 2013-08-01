package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.CacheStoreConfiguration;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
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
import org.infinispan.loaders.decorators.ChainingCacheStore;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.loaders.spi.CacheLoader;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.loaders.decorators.AbstractDelegatingStore.undelegateCacheLoader;

@MBean(objectName = "CacheLoader", description = "Component that handles loading entries from a CacheStore into memory.")
public class CacheLoaderInterceptor extends JmxStatsCommandInterceptor {
   private final AtomicLong cacheLoads = new AtomicLong(0);
   private final AtomicLong cacheMisses = new AtomicLong(0);

   protected CacheLoaderManager clm;
   protected CacheNotifier notifier;
   protected CacheLoader loader;
   protected volatile boolean enabled = true;
   private EntryFactory entryFactory;

   private static final Log log = LogFactory.getLog(CacheLoaderInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void injectDependencies(CacheLoaderManager clm, EntryFactory entryFactory, CacheNotifier notifier) {
      this.clm = clm;
      this.notifier = notifier;
      this.entryFactory = entryFactory;
   }

   @Start(priority = 15)
   @SuppressWarnings("unused")
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

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      int totalSize = 0;
      if (enabled && !shouldSkipCacheLoader(command)) {
         // TODO: maybe we should add a size method to the loader so it can do additional optimizations?  It seems
         // awfully expensive to resurrect all keys just to count how many there are.
         totalSize = loader.loadAllKeys(Collections.emptySet()).size();
      }
      // Passivation stores evicted entries so we want to add those or if the loader didn't have anything or was skipped
      // we should at least return the in memory size
      // This assumes that when passivation is not enabled that the cache store holds a superset of the data container
      if (cacheConfiguration.loaders().passivation() || totalSize == 0) {
         totalSize += (Integer)super.visitSizeCommand(ctx, command);
      }
      return totalSize;
   }

   @Override
   public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
      Object keys = super.visitKeySetCommand(ctx, command);
      if (enabled && !shouldSkipCacheLoader(command)) {
         Set<Object> union = new HashSet<Object>((Set<Object>)keys);
         // Exclude the keys in memory as we don't want to deserialize those again
         union.addAll(loader.loadAllKeys(union));
         return Collections.unmodifiableSet(union);
      }
      return keys;
   }

   @Override
   public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
      Object entrySet = super.visitEntrySetCommand(ctx, command);
      if (enabled && !shouldSkipCacheLoader(command)) {
         Set<InternalCacheEntry> set = loader.loadAll();
         Set<InternalCacheEntry> union = new HashSet<InternalCacheEntry>(set);
         union.addAll((Set<InternalCacheEntry>)entrySet);
         return Collections.unmodifiableSet(union);
      }
      return entrySet;
   }

   @Override
   public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
      Object values = super.visitValuesCommand(ctx, command);
      if (enabled && !shouldSkipCacheLoader(command)) {
         Set<InternalCacheEntry> set = loader.loadAll();
         Collection<Object> valueCollection = (Collection<Object>)values;
         List<Object> valueList = new ArrayList<Object>(set.size() + valueCollection.size());

         valueList.addAll(valueCollection);
         for (InternalCacheEntry ice : set) {
            valueList.add(ice.getValue());
         }
         return Collections.unmodifiableList(valueList);
      }
      return values;
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

   private boolean shouldSkipCacheLoader(LocalFlagAffectedCommand cmd) {
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
   private Boolean loadIfNeeded(InvocationContext ctx, Object key, boolean isRetrieval, FlagAffectedCommand cmd) throws Throwable {
      if (shouldSkipCacheLoader(cmd) || cmd.hasFlag(Flag.IGNORE_RETURN_VALUES) || !canLoad(key)) {
         return null; //skip operation
      }

      // If this is a remote call, skip loading UNLESS we are the primary data owner of this key, and
      // are using eviction or write skew checking.
      if (!isRetrieval && !ctx.isOriginLocal() && !forceLoad(key, cmd.getFlags())) return null;

      // first check if the container contains the key we need.  Try and load this into the context.
      CacheEntry e = ctx.lookupEntry(key);
      if (e == null || e.isNull() || e.getValue() == null) {
         InternalCacheEntry loaded = loader.load(key);
         if (loaded != null) {
            CacheEntry wrappedEntry;
            if (cmd instanceof ApplyDeltaCommand) {
               ctx.putLookedUpEntry(key, loaded);
               wrappedEntry = entryFactory.wrapEntryForDelta(ctx, key, ((ApplyDeltaCommand)cmd).getDelta());
            } else {
               wrappedEntry = entryFactory.wrapEntryForPut(ctx, key, loaded, false, cmd, false);
            }
            recordLoadedEntry(ctx, key, wrappedEntry, loaded, cmd);
            return Boolean.TRUE;
         } else {
            return Boolean.FALSE;
         }
      } else {
         return null;
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
         CacheEntry entry, InternalCacheEntry loadedEntry, FlagAffectedCommand cmd) throws Exception {
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
         sendNotification(key, value, true, ctx, cmd);
         entry.setValue(value);

         Metadata metadata = cmd.getMetadata();
         Metadata loadedMetadata = loadedEntry.getMetadata();
         if (metadata != null && loadedMetadata != null)
            metadata = Metadatas.applyVersion(loadedMetadata, metadata);
         else if (metadata == null)
            metadata = loadedMetadata;

         entry.setMetadata(metadata);
         // TODO shouldn't we also be setting last used and created timestamps?
         entry.setValid(true);
         entry.setLoaded(true); // mark the entry as loaded from the store

         sendNotification(key, value, false, ctx, cmd);
      }
   }

   protected void sendNotification(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand cmd) {
      notifier.notifyCacheEntryLoaded(key, value, pre, ctx, cmd);
   }

   private void loadIfNeededAndUpdateStats(InvocationContext ctx, Object key, boolean isRetrieval, FlagAffectedCommand cmd) throws Throwable {
      Boolean found = loadIfNeeded(ctx, key, isRetrieval, cmd);
      if (found == Boolean.FALSE && getStatisticsEnabled()) {
         cacheMisses.incrementAndGet();
      }
   }

   @ManagedAttribute(
         description = "Number of entries loaded from cache store",
         displayName = "Number of cache store loads",
         measurementType = MeasurementType.TRENDSUP
   )
   @SuppressWarnings("unused")
   public long getCacheLoaderLoads() {
      return cacheLoads.get();
   }

   @ManagedAttribute(
         description = "Number of entries that did not exist in cache store",
         displayName = "Number of cache store load misses",
         measurementType = MeasurementType.TRENDSUP
   )
   @SuppressWarnings("unused")
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
   @SuppressWarnings("unused")
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
