package org.infinispan.container;

import org.infinispan.configuration.cache.VersioningScheme;
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
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.metadata.Metadatas;
import org.infinispan.notifications.cachelistener.CacheNotifier;
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
   protected boolean clusterModeWriteSkewCheck;
   private Configuration configuration;
   private CacheNotifier notifier;

   @Inject
   public void injectDependencies(DataContainer dataContainer, Configuration configuration, CacheNotifier notifier) {
      this.container = dataContainer;
      this.configuration = configuration;
      this.notifier = notifier;
   }

   @Start (priority = 8)
   public void init() {
      useRepeatableRead = configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      clusterModeWriteSkewCheck = useRepeatableRead && configuration.locking().writeSkewCheck() &&
            configuration.clustering().cacheMode().isClustered() && configuration.versioning().scheme() == VersioningScheme.SIMPLE &&
            configuration.versioning().enabled();
   }

   @Override
   public final CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         cacheEntry = getFromContainer(key);

         // do not bother wrapping though if this is not in a tx.  repeatable read etc are all meaningless unless there is a tx.
         if (useRepeatableRead) {
            MVCCEntry mvccEntry;
            if (cacheEntry == null) {
               mvccEntry = createWrappedEntry(key, null, ctx, null, false, false, false);
            } else {
               mvccEntry = createWrappedEntry(key, cacheEntry, ctx, null, false, false, false);
               // If the original entry has changeable state, copy state flags to the new MVCC entry.
               if (cacheEntry instanceof StateChangingEntry && mvccEntry != null)
                  mvccEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);
            }

            if (mvccEntry != null) ctx.putLookedUpEntry(key, mvccEntry);
            if (trace) {
               log.tracef("Wrap %s for read. Entry=%s", key, mvccEntry);
            }
            return mvccEntry;
         } else if (cacheEntry != null) { // if not in transaction and repeatable read, or simply read committed (regardless of whether in TX or not), do not wrap
            ctx.putLookedUpEntry(key, cacheEntry);
         }
         if (trace) {
            log.tracef("Wrap %s for read. Entry=%s", key, cacheEntry);
         }
         return cacheEntry;
      }
      if (trace) {
         log.tracef("Wrap %s for read. Entry=%s", key, cacheEntry);
      }
      return cacheEntry;
   }

   @Override
   public final  MVCCEntry wrapEntryForClear(InvocationContext ctx, Object key) throws InterruptedException {
      //skipRead == true because the keys values are not read during the ClearOperation (neither by application)
      MVCCEntry mvccEntry = wrapEntry(ctx, key, null, true);
      if (trace) {
         log.tracef("Wrap %s for clear. Entry=%s", key, mvccEntry);
      }
      return mvccEntry;
   }

   @Override
   public final  MVCCEntry wrapEntryForReplace(InvocationContext ctx, ReplaceCommand cmd) throws InterruptedException {
      Object key = cmd.getKey();
      MVCCEntry mvccEntry = wrapEntry(ctx, key, cmd.getMetadata(), false);
      if (mvccEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      }
      if (trace) {
         log.tracef("Wrap %s for replace. Entry=%s", key, mvccEntry);
      }
      return mvccEntry;
   }

   @Override
   public final  MVCCEntry wrapEntryForRemove(InvocationContext ctx, Object key, boolean skipRead) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry = null;
      if (cacheEntry != null) {
         if (cacheEntry instanceof MVCCEntry) {
            mvccEntry = (MVCCEntry) cacheEntry;
         } else {
            //skipRead == true because the key already exists in the context that means the key was previous accessed.
            mvccEntry = wrapMvccEntryForRemove(ctx, key, cacheEntry, true);
         }
      } else {
         InternalCacheEntry ice = getFromContainer(key);
         if (ice != null || clusterModeWriteSkewCheck) {
            mvccEntry = wrapInternalCacheEntryForPut(ctx, key, ice, null, skipRead);
         }
      }
      if (mvccEntry == null) {
         // make sure we record this! Null value since this is a forced lock on the key
         ctx.putLookedUpEntry(key, null);
      } else {
         mvccEntry.copyForUpdate(container);
      }
      if (trace) {
         log.tracef("Wrap %s for remove. Entry=%s", key, mvccEntry);
      }
      return mvccEntry;
   }

   @Override
   //removed final modifier to allow mock this method
   public MVCCEntry wrapEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry icEntry,
         boolean undeleteIfNeeded, FlagAffectedCommand cmd, boolean skipRead) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry;
      if (cacheEntry != null && cacheEntry.isNull() && !useRepeatableRead) cacheEntry = null;
      Metadata providedMetadata = cmd.getMetadata();
      if (cacheEntry != null) {
         if (useRepeatableRead) {
            //sanity check. In repeatable read, we only deal with RepeatableReadEntry and ClusteredRepeatableReadEntry
            if (cacheEntry instanceof RepeatableReadEntry) {
               mvccEntry = (MVCCEntry) cacheEntry;
            } else {
               throw new IllegalStateException("Cache entry stored in context should be a RepeatableReadEntry instance " +
                                                     "but it is " + cacheEntry.getClass().getCanonicalName());
            }
            //if the icEntry is not null, then this is a remote get. We need to update the value and the metadata.
            if (!mvccEntry.isRemoved() && !mvccEntry.skipRemoteGet() && icEntry != null) {
               mvccEntry.setValue(icEntry.getValue());
               updateVersion(mvccEntry, icEntry.getMetadata());
            }
            if (!mvccEntry.isRemoved() && mvccEntry.isNull()) {
               //new entry
               mvccEntry.setCreated(true);
            }
            //always update the metadata if needed.
            updateMetadata(mvccEntry, providedMetadata);

         } else {
            //skipRead == true because the key already exists in the context that means the key was previous accessed.
            mvccEntry = wrapMvccEntryForPut(ctx, key, cacheEntry, providedMetadata, true);
         }
         mvccEntry.undelete(undeleteIfNeeded);
      } else {
         InternalCacheEntry ice = (icEntry == null ? getFromContainer(key) : icEntry);
         // A putForExternalRead is putIfAbsent, so if key present, do nothing
         if (ice != null && cmd.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
            // make sure we record this! Null value since this is a forced lock on the key
            ctx.putLookedUpEntry(key, null);
            if (trace) {
               log.tracef("Wrap %s for put. Entry=null", key);
            }
            return null;
         }

         mvccEntry = ice != null ?
             wrapInternalCacheEntryForPut(ctx, key, ice, providedMetadata, skipRead) :
             newMvccEntryForPut(ctx, key, cmd, providedMetadata, skipRead);
      }
      mvccEntry.copyForUpdate(container);
      if (trace) {
         log.tracef("Wrap %s for put. Entry=%s", key, mvccEntry);
      }
      return mvccEntry;
   }
   
   @Override
   public CacheEntry wrapEntryForDelta(InvocationContext ctx, Object deltaKey, Delta delta ) throws InterruptedException {
      CacheEntry cacheEntry = getFromContext(ctx, deltaKey);
      DeltaAwareCacheEntry deltaAwareEntry = null;
      if (cacheEntry != null) {        
         deltaAwareEntry = wrapEntryForDelta(ctx, deltaKey, cacheEntry);
      } else {                     
         InternalCacheEntry ice = getFromContainer(deltaKey);
         if (ice != null){
            deltaAwareEntry = newDeltaAwareCacheEntry(ctx, deltaKey, (DeltaAware)ice.getValue());
         }
      }
      if (deltaAwareEntry != null)
         deltaAwareEntry.appendDelta(delta);
      if (trace) {
         log.tracef("Wrap %s for delta. Entry=%s", deltaKey, deltaAwareEntry);
      }
      return deltaAwareEntry;
   }
   
   private DeltaAwareCacheEntry wrapEntryForDelta(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      if (cacheEntry instanceof DeltaAwareCacheEntry) return (DeltaAwareCacheEntry) cacheEntry;
      return wrapInternalCacheEntryForDelta(ctx, key, cacheEntry);
   }
   
   private DeltaAwareCacheEntry wrapInternalCacheEntryForDelta(InvocationContext ctx, Object key, CacheEntry cacheEntry) {
      DeltaAwareCacheEntry e;
      if(cacheEntry instanceof MVCCEntry){
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), cacheEntry);
      }
      else if (cacheEntry instanceof InternalCacheEntry) {
         cacheEntry = wrapInternalCacheEntryForPut(ctx, key, (InternalCacheEntry) cacheEntry, null, false);
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), cacheEntry);
      }
      else {
         e = createWrappedDeltaEntry(key, (DeltaAware) cacheEntry.getValue(), null);
      }
      ctx.putLookedUpEntry(key, e);
      return e;

   }

   private CacheEntry getFromContext(InvocationContext ctx, Object key) {
      final CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (trace) log.tracef("Exists in context? %s ", cacheEntry);
      return cacheEntry;
   }

   private InternalCacheEntry getFromContainer(Object key) {
      final InternalCacheEntry ice = container.get(key);
      if (trace) log.tracef("Retrieved from container %s", ice);
      return ice;
   }

   private MVCCEntry newMvccEntryForPut(
         InvocationContext ctx, Object key, FlagAffectedCommand cmd, Metadata providedMetadata, boolean skipRead) {
      MVCCEntry mvccEntry;
      if (trace) log.trace("Creating new entry.");
      notifier.notifyCacheEntryCreated(key, null, true, ctx, cmd);
      mvccEntry = createWrappedEntry(key, null, ctx, providedMetadata, true, false, skipRead);
      mvccEntry.setCreated(true);
      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapMvccEntryForPut(InvocationContext ctx, Object key, CacheEntry cacheEntry, Metadata providedMetadata, boolean skipRead) {
      if (cacheEntry instanceof MVCCEntry) {
         MVCCEntry mvccEntry = (MVCCEntry) cacheEntry;
         updateMetadata(mvccEntry, providedMetadata);
         return mvccEntry;
      }
      return wrapInternalCacheEntryForPut(ctx, key, (InternalCacheEntry) cacheEntry, providedMetadata, skipRead);
   }

   private MVCCEntry wrapInternalCacheEntryForPut(InvocationContext ctx, Object key, InternalCacheEntry cacheEntry, Metadata providedMetadata, boolean skipRead) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry, ctx, providedMetadata, false, false, skipRead);
      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapMvccEntryForRemove(InvocationContext ctx, Object key, CacheEntry cacheEntry, boolean skipRead) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry, ctx, null, false, true, skipRead);
      // If the original entry has changeable state, copy state flags to the new MVCC entry.
      if (cacheEntry instanceof StateChangingEntry)
         mvccEntry.copyStateFlagsFrom((StateChangingEntry) cacheEntry);

      ctx.putLookedUpEntry(key, mvccEntry);
      return mvccEntry;
   }

   private MVCCEntry wrapEntry(InvocationContext ctx, Object key, Metadata providedMetadata, boolean skipRead) {
      CacheEntry cacheEntry = getFromContext(ctx, key);
      MVCCEntry mvccEntry = null;
      if (cacheEntry != null) {
         //already wrapped. set skip read to true to avoid replace the current version.
         mvccEntry = wrapMvccEntryForPut(ctx, key, cacheEntry, providedMetadata, true);
      } else {
         InternalCacheEntry ice = getFromContainer(key);
         if (ice != null || clusterModeWriteSkewCheck) {
            mvccEntry = wrapInternalCacheEntryForPut(ctx, key, ice, providedMetadata, skipRead);
         }
      }
      if (mvccEntry != null)
         mvccEntry.copyForUpdate(container);
      return mvccEntry;
   }

   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry, InvocationContext context,
                                          Metadata providedMetadata, boolean isForInsert, boolean forRemoval, boolean skipRead) {
      Object value = cacheEntry != null ? cacheEntry.getValue() : null;
      Metadata metadata = providedMetadata != null
            ? providedMetadata
            : cacheEntry != null ? cacheEntry.getMetadata() : null;

      if (value == null && !isForInsert && !useRepeatableRead)
         return null;

      return useRepeatableRead
            ? new RepeatableReadEntry(key, value, metadata)
            : new ReadCommittedEntry(key, value, metadata);
   }
   
   private DeltaAwareCacheEntry newDeltaAwareCacheEntry(InvocationContext ctx, Object key, DeltaAware deltaAware){
      DeltaAwareCacheEntry deltaEntry = createWrappedDeltaEntry(key, deltaAware, null);
      ctx.putLookedUpEntry(key, deltaEntry);
      return deltaEntry;
   }
   
   private DeltaAwareCacheEntry createWrappedDeltaEntry(Object key, DeltaAware deltaAware, CacheEntry entry) {
      return new DeltaAwareCacheEntry(key,deltaAware, entry);
   }

   private void updateMetadata(MVCCEntry entry, Metadata providedMetadata) {
      if (trace) {
         log.tracef("Update metadata for %s. Provided metadata is %s", entry, providedMetadata);
      }
      if (providedMetadata == null || entry == null || entry.getMetadata() != null) {
         return;
      }
      entry.setMetadata(providedMetadata);
   }

   private void updateVersion(MVCCEntry entry, Metadata providedMetadata) {
      if (trace) {
         log.tracef("Update metadata for %s. Provided metadata is %s", entry, providedMetadata);
      }
      if (providedMetadata == null || entry == null) {
         return;
      } else if (entry.getMetadata() == null) {
         entry.setMetadata(providedMetadata);
         return;
      }

      entry.setMetadata(Metadatas.applyVersion(entry.getMetadata(), providedMetadata));
   }

}
