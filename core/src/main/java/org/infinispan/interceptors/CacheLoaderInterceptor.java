package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.group.GroupFilter;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.persistence.PersistenceUtil.convert;

@MBean(objectName = "CacheLoader", description = "Component that handles loading entries from a CacheStore into memory.")
public class CacheLoaderInterceptor extends JmxStatsCommandInterceptor {
   private final AtomicLong cacheLoads = new AtomicLong(0);
   private final AtomicLong cacheMisses = new AtomicLong(0);

   protected PersistenceManager persistenceManager;
   protected CacheNotifier notifier;
   protected volatile boolean enabled = true;
   protected EntryFactory entryFactory;
   private TimeService timeService;
   private InternalEntryFactory iceFactory;
   private DataContainer dataContainer;
   private GroupManager groupManager;

   private static final Log log = LogFactory.getLog(CacheLoaderInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void injectDependencies(PersistenceManager clm, EntryFactory entryFactory, CacheNotifier notifier,
                                     TimeService timeService, InternalEntryFactory iceFactory, DataContainer dataContainer,
                                     GroupManager groupManager) {
      this.persistenceManager = clm;
      this.notifier = notifier;
      this.entryFactory = entryFactory;
      this.timeService = timeService;
      this.iceFactory = iceFactory;
      this.dataContainer = dataContainer;
      this.groupManager = groupManager;
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeeded(ctx, key, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeeded(ctx, key, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }


   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeededAndUpdateStats(ctx, key, command);
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
               loadIfNeeded(ctx, key, command);
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
            loadIfNeededAndUpdateStats(ctx, key, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (enabled) {
         Object key;
         if ((key = command.getKey()) != null) {
            loadIfNeededAndUpdateStats(ctx, key, command);
         }
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitGetKeysInGroupCommand(final InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      if (!command.isGroupOwner() || !enabled || hasSkipLoadFlag(command)) {
         return invokeNextInterceptor(ctx, command);
      }

      final KeyFilter<Object> keyFilter = new CompositeKeyFilter<>(new GroupFilter<>(groupName, groupManager),
                                                                   new CollectionKeyFilter<>(ctx.getLookedUpEntries().keySet()));
      persistenceManager.processOnAllStores(keyFilter, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            synchronized (ctx) {
               //the process can be made in multiple threads, so we need to synchronize in the context.
               entryFactory.wrapEntryForReading(ctx, marshalledEntry.getKey(), convert(marshalledEntry, iceFactory)).setSkipLookup(true);
            }
         }
      }, true, true);
      return invokeNextInterceptor(ctx, command);
   }

   /**
    * Indicates whether the operation is a delta write. If it is, the
    * previous value needs to be loaded from the cache store so that
    * it can be merged.
    */
   protected final boolean isDeltaWrite(WriteCommand cmd) {
      return cmd.hasFlag(Flag.DELTA_WRITE);
   }

   protected final boolean isConditional(WriteCommand cmd) {
      return cmd.isConditional();
   }

   protected final boolean hasSkipLoadFlag(LocalFlagAffectedCommand cmd) {
      return cmd.hasFlag(Flag.SKIP_CACHE_LOAD);
   }

   protected final boolean hasIgnoreReturnValueFlag(LocalFlagAffectedCommand cmd) {
      return cmd.hasFlag(Flag.IGNORE_RETURN_VALUES);
   }

   protected boolean canLoad(Object key) {
      return true;
   }

   /**
    * Loads from the cache loader the entry for the given key.  A found value is loaded into the current context.  The
    * method returns whether the value was found or not, or even if the cache loader was checked.
    * @param ctx The current invocation's context
    * @param key The key for the entry to look up
    * @param cmd The command that was called that now wants to query the cache loader
    * @return Whether or not the entry was found in the cache loader.  A value of null means the cache loader was never
    *         queried for the value, so it was neither a hit or a miss.
    * @throws Throwable
    */
   protected final Boolean loadIfNeeded(final InvocationContext ctx, Object key, final FlagAffectedCommand cmd) throws Throwable {
      if (skipLoad(cmd, key, ctx)) {
         return null;
      }

      final boolean isDelta = cmd instanceof ApplyDeltaCommand;
      final AtomicReference<Boolean> isLoaded = new AtomicReference<>();
      InternalCacheEntry entry = PersistenceUtil.loadAndStoreInDataContainer(dataContainer, persistenceManager, key,
                                                                             ctx, timeService, isLoaded);
      if (entry != null) {
         CacheEntry wrappedEntry = wrapInternalCacheEntry(ctx, key, cmd, entry, isDelta);
         if (isLoaded.get() == Boolean.TRUE) {
            recordLoadedEntry(ctx, key, wrappedEntry, entry, cmd);
         }

      }
      return isLoaded.get();
   }

   private boolean skipLoad(FlagAffectedCommand cmd, Object key, InvocationContext ctx) {
      if (!shouldAttemptLookup(ctx.lookupEntry(key))) {
         if (trace) {
            log.tracef("Skip load for command %s. Entry already exists in context.", cmd);
         }
         return true;
      }

      if (!canLoad(key)) {
         if (trace) {
            log.tracef("Skip load for command %s. Cannot load the key.", cmd);
         }
         return true;
      }

      boolean skip;
      if (cmd instanceof WriteCommand) {
         skip = skipLoadForWriteCommand((WriteCommand) cmd, key, ctx);
         if (trace) {
            log.tracef("Skip load for write command %s? %s", cmd, skip);
         }
      } else {
         //read command
         skip = hasSkipLoadFlag(cmd);
         if (trace) {
            log.tracef("Skip load for command %s?. %s", cmd, skip);
         }
      }
      return skip;
   }

   protected boolean skipLoadForWriteCommand(WriteCommand cmd, Object key, InvocationContext ctx) {
      if (isDeltaWrite(cmd)) {
         if (trace) {
            log.tracef("Don't skip load for command %s. Value is needed!", cmd);
         }
         return false;
      } else if (isConditional(cmd)) {
         boolean skip = hasSkipLoadFlag(cmd);
         if (trace) {
            log.tracef("Skip load for conditional command %s? %s", cmd, skip);
         }
         return skip;
      }
      return hasSkipLoadFlag(cmd) || hasIgnoreReturnValueFlag(cmd);
   }

   private CacheEntry wrapInternalCacheEntry(InvocationContext ctx, Object key, FlagAffectedCommand cmd,
                                             InternalCacheEntry ice, boolean isDelta) {
      if (isDelta) {
         ctx.putLookedUpEntry(key, ice);
         return entryFactory.wrapEntryForDelta(ctx, key, ((ApplyDeltaCommand) cmd).getDelta());
      } else {
         return entryFactory.wrapEntryForPut(ctx, key, ice, false, cmd, false);
      }
   }

   /**
    * Only perform if context doesn't have a value found (Read Committed) or if we can do a remote
    * get only if the value is null (Repeatable Read)
    */
   private boolean shouldAttemptLookup(CacheEntry e) {
      return e == null || (e.isNull() || e.getValue() == null) && !e.skipLookup();
   }

   /**
    * This method records a loaded entry, performing the following steps: <ol> <li>Increments counters for reporting via
    * JMX</li> <li>updates the 'entry' reference (an entry in the current thread's InvocationContext) with the contents
    * of 'loadedEntry' (freshly loaded from the CacheStore) so that the loaded details will be flushed to the
    * DataContainer when the call returns (in the LockingInterceptor, when locks are released)</li> <li>Notifies
    * listeners</li> </ol>
    */
   private void recordLoadedEntry(InvocationContext ctx, Object key,
                                  CacheEntry entry, InternalCacheEntry loadedEntry, FlagAffectedCommand cmd) {
      boolean entryExists = loadedEntry != null;
      if (trace) {
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

         sendNotification(key, value, false, ctx, cmd);
      }
   }

   protected void sendNotification(Object key, Object value, boolean pre,
         InvocationContext ctx, FlagAffectedCommand cmd) {
      notifier.notifyCacheEntryLoaded(key, value, pre, ctx, cmd);
   }

   private void loadIfNeededAndUpdateStats(InvocationContext ctx, Object key, FlagAffectedCommand cmd) throws Throwable {
      Boolean found = loadIfNeeded(ctx, key, cmd);
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
         description = "Returns a collection of cache loader types which are configured and enabled",
         displayName = "Returns a collection of cache loader types which are configured and enabled",
         displayType = DisplayType.DETAIL)
   /**
    * This method returns a collection of cache loader types (fully qualified class names) that are configured and enabled.
    */
   public Collection<String> getStores() {
      if (enabled && cacheConfiguration.persistence().usingStores()) {
         return persistenceManager.getStoresAsString();
      } else {
         return InfinispanCollections.emptySet();
      }
   }

   @ManagedOperation(
         description = "Disable all stores of a given type, where type is a fully qualified class name of the cache loader to disable",
         displayName = "Disable all stores of a given type"
   )
   @SuppressWarnings("unused")
   /**
    * Disables a store of a given type.
    *
    * If the given type cannot be found, this is a no-op.  If more than one store of the same type is configured,
    * all stores of the given type are disabled.
    *
    * @param storeType fully qualified class name of the cache loader type to disable
    */
   public void disableStore(@Parameter(name = "storeType", description = "Fully qualified class name of a store implementation") String storeType) {
      if (enabled) persistenceManager.disableStore(storeType);
   }

   public void disableInterceptor() {
      enabled = false;
   }
}
