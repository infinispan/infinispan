package org.infinispan.container;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.DeltaAwareCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
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
   private final static boolean trace = log.isTraceEnabled();

   private boolean useRepeatableRead;
   private DataContainer container;
   private boolean isL1Enabled;
   private Configuration configuration;
   private PersistenceManager persistenceManager; // hack for DeltaAware
   private TimeService timeService;

   @Inject
   public void injectDependencies(DataContainer dataContainer, Configuration configuration,
                                  TimeService timeService, PersistenceManager persistenceManager) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.timeService = timeService;
      this.persistenceManager = persistenceManager;
   }

   @Start (priority = 8)
   public void init() {
      useRepeatableRead = configuration.transaction().transactionMode().isTransactional()
            && configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      isL1Enabled = configuration.clustering().l1().enabled();
   }

   @Override
   public final void wrapEntryForReading(InvocationContext ctx, Object key, boolean isOwner) {
      if (!isOwner && !isL1Enabled) {
         return;
      }
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         cacheEntry = getFromContainer(key, isOwner, false);

         if (cacheEntry != null) {
            // With repeatable read, we need to create a RepeatableReadEntry as internal cache entries are mutable
            // Otherwise we can store the InternalCacheEntry directly in the context
            if (useRepeatableRead) {
               cacheEntry = createWrappedEntry(key, cacheEntry, ctx, false);
            }
            ctx.putLookedUpEntry(key, cacheEntry);
         }
      }

      if (trace) {
         log.tracef("Wrap %s for read. Entry=%s", toStr(key), cacheEntry);
      }
      return;
   }

   @Override
   public MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean skipRead, boolean isOwner) {
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
         CacheEntry cacheEntry = getFromContainer(key, isOwner, true);
         if (cacheEntry == null) {
            return null;
         }
         mvccEntry = createWrappedEntry(key, cacheEntry, ctx, skipRead);
         if (cacheEntry.isNull()) {
            mvccEntry.setCreated(true);
         }
         ctx.putLookedUpEntry(key, mvccEntry);
         if (trace)
            log.tracef("Updated context entry %s", mvccEntry);
      }
      if (mvccEntry != null) {
         mvccEntry.copyForUpdate();
      }
      return mvccEntry;
   }

   @Override
   public boolean wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, boolean isWrite,
                                    boolean skipRead) {
      // For a write operation, the entry is always already wrapped. For a read operation, the entry may be
      // in the context as an InternalCacheEntry, as null, or missing altogether.
      CacheEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         // Already wrapped for a write. Update the value and the metadata.
         if (contextEntry.skipLookup()) {
            // This can happen during getGroup() invocations, which request the whole group from remote nodes
            // even if some keys are already in the context.
            if (trace)
               log.tracef("Ignored update for context entry %s", contextEntry);
            return false;
         }
         contextEntry.setValue(externalEntry.getValue());
         contextEntry.setCreated(externalEntry.getCreated());
         contextEntry.setLastUsed(externalEntry.getLastUsed());
         contextEntry.setMetadata(externalEntry.getMetadata());
         if (useRepeatableRead) {
            contextEntry.setSkipLookup(true);
         }
         if (trace)
            log.tracef("Updated context entry %s", contextEntry);
         return true;
      } else if (contextEntry == null || contextEntry.isNull()) {
         if (isWrite || useRepeatableRead) {
            MVCCEntry mvccEntry = createWrappedEntry(key, externalEntry, ctx, skipRead);
            ctx.putLookedUpEntry(key, mvccEntry);
            if (trace)
               log.tracef("Updated context entry %s", mvccEntry);
         } else {
            // This is a read operation, store the external entry in the context directly.
            ctx.putLookedUpEntry(key, externalEntry);
            if (trace)
               log.tracef("Updated context entry %s", externalEntry);
         }
         return true;
      } else {
         // TODO: maybe isValid check instead?
         if (useRepeatableRead) {
            if (trace) log.tracef("Ingored update to %s -> %s as we do repeatable reads", contextEntry, externalEntry);
            return false;
         } else {
            ctx.putLookedUpEntry(key, externalEntry);
            if (trace) log.tracef("Updated context entry %s", externalEntry);
            return true;
         }
      }
   }

   @Override
   public CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta, boolean isOwner) {
      CacheEntry cacheEntry = getFromContext(ctx, deltaKey);
      DeltaAwareCacheEntry deltaAwareEntry;
      if (cacheEntry instanceof DeltaAwareCacheEntry) {
         // Already delta-aware, nothing to do.
         deltaAwareEntry = (DeltaAwareCacheEntry) cacheEntry;
      } else if (cacheEntry != null) {
         // Wrap the existing context entry inside a DeltaAwareCacheEntry
         deltaAwareEntry = createWrappedDeltaEntry(deltaKey, (DeltaAware) cacheEntry.getValue(), cacheEntry, ctx);
         ctx.putLookedUpEntry(deltaKey, deltaAwareEntry);
      } else {
         // Read the value from the container and wrap it
         cacheEntry = getFromContainer(deltaKey, isOwner, false);
         DeltaAwareCacheEntry deltaEntry =
               createWrappedDeltaEntry(deltaKey, cacheEntry != null ? (DeltaAware) cacheEntry.getValue() : null, null, ctx);

         ctx.putLookedUpEntry(deltaKey, deltaEntry);
         deltaAwareEntry = deltaEntry;
      }
      if (trace) log.tracef("Wrap %s for delta. Entry=%s", deltaKey, deltaAwareEntry);
      return deltaAwareEntry;
   }

   private MVCCEntry assertRepeatableReadEntry(CacheEntry cacheEntry) {
      // Sanity check. In repeatable read, we only use RepeatableReadEntry and ClusteredRepeatableReadEntry
      if (useRepeatableRead && !(cacheEntry instanceof RepeatableReadEntry || cacheEntry instanceof DeltaAwareCacheEntry)) {
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

   private CacheEntry getFromContainer(Object key, boolean shouldLoad, boolean writeOperation) {
      if (shouldLoad) {
         final InternalCacheEntry ice = innerGetFromContainer(key, writeOperation);
         if (trace)
            log.tracef("Retrieved from container %s", ice);
         if (ice == null) {
            return NullCacheEntry.getInstance();
         }
         return ice;
      } else if (isL1Enabled) {
         final InternalCacheEntry ice = innerGetFromContainer(key, writeOperation);
         if (trace)
            log.tracef("Retrieved from container %s", ice);
         if (ice == null || !ice.isL1Entry()) return null;
         return ice;
      }
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

      if (trace) log.tracef("Creating new entry for key %s", toStr(key));
      MVCCEntry mvccEntry = useRepeatableRead ? new RepeatableReadEntry(key, value, metadata) :
            new ReadCommittedEntry(key, value, metadata);

      mvccEntry.setSkipLookup(cacheEntry != null && cacheEntry.skipLookup());
      return mvccEntry;
   }

   private DeltaAwareCacheEntry createWrappedDeltaEntry(Object key, DeltaAware deltaAware, CacheEntry entry, InvocationContext ctx) {
      DeltaAwareCacheEntry deltaAwareCacheEntry = new DeltaAwareCacheEntry(key, deltaAware, entry, ctx, persistenceManager, timeService);
      // Set the delta aware entry to created so it ignores the previous value and only merges new deltas when it is
      // committed
      if (entry == null) {
         deltaAwareCacheEntry.setSkipLookup(false);
      } else if (entry.isCreated()) {
         deltaAwareCacheEntry.setCreated(true);
      }
      return deltaAwareCacheEntry;
   }
}
