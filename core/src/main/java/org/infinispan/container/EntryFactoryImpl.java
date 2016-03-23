package org.infinispan.container;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.container.entries.StateChangingEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link EntryFactory} implementation to be used for optimistic locking scheme.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class EntryFactoryImpl implements EntryFactory {

   private static final Log log = LogFactory.getLog(EntryFactoryImpl.class);
   private final boolean trace = log.isTraceEnabled();
   
   protected boolean useRepeatableRead;
   private DataContainer container;
   private boolean isL1Enabled; //cache the value
   private Configuration configuration;
   private DistributionManager distributionManager;//is null for non-clustered caches
   private TimeService timeService;

   @Inject
   public void injectDependencies(DataContainer dataContainer, Configuration configuration,
                                  DistributionManager distributionManager,
                                  TimeService timeService) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.distributionManager = distributionManager;
      this.timeService = timeService;
   }

   @Start (priority = 8)
   public void init() {
      useRepeatableRead = configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      isL1Enabled = configuration.clustering().l1().enabled();
   }

   @Override
   public final CacheEntry wrapEntryForReading(InvocationContext ctx, Object key, CacheEntry existing) {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         cacheEntry = existing != null ? existing : getFromContainer(key, false, false);

         // With repeatable read, we need to create a RepeatableReadEntry
         // Otherwise we can store the InternalCacheEntry directly in the context
         if (useRepeatableRead) {
            cacheEntry = createWrappedEntry(key, cacheEntry, ctx, false);
         }
         if (cacheEntry != null) {
            ctx.putLookedUpEntry(key, cacheEntry);
         }
      }

      if (trace) {
         log.tracef("Wrap %s for read. Entry=%s", key, cacheEntry);
      }
      return cacheEntry;
   }

   @Override
   public MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, Wrap wrap, boolean skipRead,
                                        boolean ignoreOwnership) {
      if (wrap == Wrap.STORE) {
         throw new IllegalStateException("wrapEntryForWriting must create a MVCCEntry");
      }
      if (useRepeatableRead) {
         wrap = Wrap.WRAP_ALL;
      }
      CacheEntry contextEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry;
      if (contextEntry instanceof MVCCEntry) {
         // Nothing to do, already wrapped.
         mvccEntry = assertRepeatableReadEntry(contextEntry);
      } else if (contextEntry != null) {
         // Already in the context as an InternalCacheEntry or DeltaAwareCacheEntry.
         // Need to wrap it in a MVCCEntry.
         mvccEntry = createWrappedEntry(key, contextEntry, ctx, skipRead);
         ctx.putLookedUpEntry(key, mvccEntry);
         if (trace)
            log.tracef("Updated context entry %s", contextEntry);
      } else {
         // Not in the context yet.
         InternalCacheEntry ice = getFromContainer(key, ignoreOwnership, true);
         if (ice == null && wrap == Wrap.WRAP_NON_NULL) {
            mvccEntry = null;
         } else {
            mvccEntry = createWrappedEntry(key, ice, ctx, skipRead);
            // TODO This will also wrap entries not owned by the local node, maybe we can avoid it for non-tx
            if (ice == null) {
               mvccEntry.setCreated(true);
            }
            ctx.putLookedUpEntry(key, mvccEntry);
            if (trace)
               log.tracef("Updated context entry %s", mvccEntry);
         }
      }
      if (mvccEntry != null) {
         mvccEntry.copyForUpdate();
      }
      return mvccEntry;
   }

   @Override
   public boolean wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, Wrap wrap,
                                    boolean skipRead) {
      // For a write operation, the entry is always already wrapped. For a read operation, the entry may be
      // in the context as an InternalCacheEntry, as null, or missing altogether.
      CacheEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         // Already wrapped for a write. Update the value and the metadata.
         if (!contextEntry.isNull() || contextEntry.skipLookup()) {
            // This can happen during getGroup() invocations, which request the whole group from remote nodes
            // even if some keys are already in the context.
            if (trace)
               log.tracef("Ignored update for context entry %s", contextEntry);
            return false;
         }
         contextEntry.setValue(externalEntry.getValue());
         contextEntry.setMetadata(externalEntry.getMetadata());
         if (trace)
            log.tracef("Updated context entry %s", contextEntry);
         return true;
      } else if (contextEntry instanceof DeltaAwareCacheEntry) {
         // Already wrapped for an ApplyDeltaCommand. Update the value.
         if (contextEntry.getValue() != null) {
            if (trace)
               log.tracef("Ignored update for context entry %s", contextEntry);
            return false;
         }
         contextEntry.setValue(externalEntry.getValue());
         contextEntry.setMetadata(externalEntry.getMetadata());
         if (trace)
            log.tracef("Updated context entry %s", contextEntry);
         return true;
      } else if (contextEntry == null || contextEntry.isNull()) {
         // Not in the context yet (or NullCacheEntry in context).
         // This shouldn't be necessary: with repeatable read, we never reach this branch, because we already have an MVCCEntry.
         if (useRepeatableRead) {
            wrap = Wrap.WRAP_ALL;
         }
         if (externalEntry == null && wrap != Wrap.WRAP_ALL) {
            // TODO We only need the wrap != WRAP_ALL check for getAll, we should remove it
            if (trace)
               log.tracef("Skipping update with null value for key %s", key);
            return false;
         }
         if (wrap == Wrap.STORE) {
            // This is a read operation, store the external entry in the context directly.
            ctx.putLookedUpEntry(key, externalEntry);
         } else {
            // This is a write (or getAll) operation, wrap it.
            MVCCEntry mvccEntry = createWrappedEntry(key, externalEntry, ctx, skipRead);
            ctx.putLookedUpEntry(key, mvccEntry);
         }
         if (trace)
            log.tracef("Updated context entry %s", externalEntry);
         return true;
      } else {
         // Already in the context as an InternalCacheEntry
         if (trace)
            log.tracef("Skipping update with null value for key %s", key);
         return false;
      }
   }

   @Override
   public CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta) {
      CacheEntry cacheEntry = getFromContext(ctx, deltaKey);
      DeltaAwareCacheEntry deltaAwareEntry;
      if (cacheEntry instanceof DeltaAwareCacheEntry) {
         // Already delta-aware, nothing to do.
         deltaAwareEntry = (DeltaAwareCacheEntry) cacheEntry;
      } else if (cacheEntry != null) {
         // Wrap the existing context entry inside a DeltaAwareCacheEntry
         deltaAwareEntry = createWrappedDeltaEntry(deltaKey, (DeltaAware) cacheEntry.getValue(), cacheEntry);
         ctx.putLookedUpEntry(deltaKey, deltaAwareEntry);
      } else {
         // Read the value from the container and wrap it
         InternalCacheEntry ice = getFromContainer(deltaKey, false, false);
         DeltaAwareCacheEntry deltaEntry =
               createWrappedDeltaEntry(deltaKey, ice != null ? (DeltaAware) ice.getValue() : null, null);

         ctx.putLookedUpEntry(deltaKey, deltaEntry);
         deltaAwareEntry = deltaEntry;
      }
      if (trace) log.tracef("Wrap %s for delta. Entry=%s", deltaKey, deltaAwareEntry);
      return deltaAwareEntry;
   }

   private MVCCEntry assertRepeatableReadEntry(CacheEntry cacheEntry) {
      // Sanity check. In repeatable read, we only use RepeatableReadEntry and ClusteredRepeatableReadEntry
      if (useRepeatableRead && !(cacheEntry instanceof RepeatableReadEntry)) {
         throw new IllegalStateException(
               "Cache entry stored in context should be a RepeatableReadEntry instance " +
                     "but it is " + cacheEntry.getClass().getCanonicalName());
      }
      return (MVCCEntry) cacheEntry;
   }

   private CacheEntry getFromContext(InvocationContext ctx, Object key) {
      final CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (trace) log.tracef("Exists in context? %s ", cacheEntry);
      return cacheEntry;
   }

   private InternalCacheEntry getFromContainer(Object key, boolean ignoreOwnership, boolean writeOperation) {
      final boolean isLocal = distributionManager == null || distributionManager.getLocality(key).isLocal();
      if (isLocal || ignoreOwnership) {
         final InternalCacheEntry ice = innerGetFromContainer(key, writeOperation);
         if (trace)
            log.tracef("Retrieved from container %s (ignoreOwnership=%s, isLocal=%s)", ice, ignoreOwnership,
                       isLocal);
         return ice;
      } else if (isL1Enabled) {
         final InternalCacheEntry ice = innerGetFromContainer(key, writeOperation);
         final boolean isL1Entry = ice != null && ice.isL1Entry();
         if (trace) log.tracef("Retrieved from container %s (L1 is enabled, isL1Entry=%s)", ice, isL1Entry);
         return isL1Entry ? ice : null;
      }
      if (trace) log.trace("Didn't retrieve from container.");
      return null;
   }

   private InternalCacheEntry innerGetFromContainer(Object key, boolean writeOperation) {
      InternalCacheEntry ice;
      // Write operations should not cause expiration events to occur, because we will most likely overwrite the
      // value anyways - also required for remove expired to not cause infinite loop
      if (writeOperation) {
         ice = container.peek(key);
         if (ice != null && ice.canExpire()) {
            long wallClockTime = timeService.wallClockTime();
            if (ice.isExpired(wallClockTime)) {
               ice = null;
            } else {
               ice.touch(wallClockTime);
            }
         }
      } else {
         ice = container.get(key);
      }
      return ice;
   }

   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry, InvocationContext context,
                                          boolean skipRead) {
      Object value = null;
      Metadata metadata = null;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         metadata = cacheEntry.getMetadata();
      }

      if (trace) log.tracef("Creating new entry for key %s", key);
      MVCCEntry mvccEntry = useRepeatableRead ? new RepeatableReadEntry(key, value, metadata) :
            new ReadCommittedEntry(key, value, metadata);

      // If the original entry has changeable state, copy state flags to the new MVCC entry.
      if (cacheEntry instanceof StateChangingEntry) {
         mvccEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);
      }
      return mvccEntry;
   }

   private DeltaAwareCacheEntry createWrappedDeltaEntry(Object key, DeltaAware deltaAware, CacheEntry entry) {
      DeltaAwareCacheEntry deltaAwareCacheEntry = new DeltaAwareCacheEntry(key, deltaAware, entry);
      // Set the delta aware entry to created so it ignores the previous value and only merges new deltas when it is
      // committed
      if (entry != null && entry.isCreated()) {
         deltaAwareCacheEntry.setCreated(true);
      }
      return deltaAwareCacheEntry;
   }
}
