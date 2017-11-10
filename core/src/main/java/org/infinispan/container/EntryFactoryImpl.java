package org.infinispan.container;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.VersionedRepeatableReadEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
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

   @Inject private DataContainer container;
   @Inject private Configuration configuration;
   @Inject private TimeService timeService;
   @Inject private VersionGenerator versionGenerator;

   private boolean isL1Enabled;
   private boolean useRepeatableRead;
   private boolean useVersioning;

   @Start (priority = 8)
   public void init() {
      // Scattered mode needs repeatable-read entries to properly retry half-committed multi-key operations
      // (see RetryingEntryWrappingInterceptor for details).
      useRepeatableRead = configuration.transaction().transactionMode().isTransactional()
            && configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ
            || configuration.clustering().cacheMode().isScattered();
      isL1Enabled = configuration.clustering().l1().enabled();
      // Write-skew check implies isolation level = REPEATABLE_READ && locking mode = OPTIMISTIC
      useVersioning = Configurations.isTxVersioned(configuration);
   }

   @Override
   public final void wrapEntryForReading(InvocationContext ctx, Object key, boolean isOwner) {
      if (!isOwner && !isL1Enabled) {
         return;
      }
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         cacheEntry = getFromContainerForRead(key, isOwner);

         if (cacheEntry != null) {
            // With repeatable read, we need to create a RepeatableReadEntry as internal cache entries are mutable
            // Otherwise we can store the InternalCacheEntry directly in the context
            if (useRepeatableRead) {
               MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry);
               mvccEntry.setRead();
               cacheEntry = mvccEntry;
            }
            ctx.putLookedUpEntry(key, cacheEntry);
         }
      }

      if (trace) {
         log.tracef("Wrap %s for read. Entry=%s", toStr(key), cacheEntry);
      }
   }

   @Override
   public void wrapEntryForWriting(InvocationContext ctx, Object key, boolean isOwner, boolean isRead) {
      CacheEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         // Nothing to do, already wrapped.
      } else if (contextEntry != null) {
         // Already in the context as an InternalCacheEntry
         // Need to wrap it in a MVCCEntry.
         MVCCEntry mvccEntry = createWrappedEntry(key, contextEntry);
         ctx.putLookedUpEntry(key, mvccEntry);
         if (trace)
            log.tracef("Updated context entry %s -> %s", contextEntry, mvccEntry);
      } else {
         // Not in the context yet.
         CacheEntry cacheEntry = getFromContainer(key, isOwner, true);
         if (cacheEntry == null) {
            return;
         }
         MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry);
         if (cacheEntry.isNull()) {
            mvccEntry.setCreated(true);
         }
         if (isRead) {
            mvccEntry.setRead();
         }
         ctx.putLookedUpEntry(key, mvccEntry);
         if (trace)
            log.tracef("Updated context entry %s -> %s", contextEntry, mvccEntry);
      }
   }

   @Override
   public void wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, boolean isRead, boolean isWrite) {
      // For a write operation, the entry is always already wrapped. For a read operation, the entry may be
      // in the context as an InternalCacheEntry, as null, or missing altogether.
      CacheEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         MVCCEntry mvccEntry = (MVCCEntry) contextEntry;
         // Already wrapped for a write. Update the value and the metadata.
         if (mvccEntry.skipLookup()) {
            // This can happen during getGroup() invocations, which request the whole group from remote nodes
            // even if some keys are already in the context.
            if (trace)
               log.tracef("Ignored update for context entry %s", contextEntry);
            return;
         }
         // Without updating initial value a local write skew check would fail when the entry is loaded
         // from the cache store. This shouldn't be called more than once since afterwards we set skipLookup
         mvccEntry.setValue(externalEntry.getValue());
         mvccEntry.setCreated(externalEntry.getCreated());
         mvccEntry.setLastUsed(externalEntry.getLastUsed());
         mvccEntry.setMetadata(externalEntry.getMetadata());
         mvccEntry.updatePreviousValue();
         if (trace) log.tracef("Updated context entry %s", contextEntry);
      } else if (contextEntry == null || contextEntry.isNull()) {
         if (isWrite || useRepeatableRead) {
            MVCCEntry mvccEntry = createWrappedEntry(key, externalEntry);
            if (isRead) {
               mvccEntry.setRead();
            }
            ctx.putLookedUpEntry(key, mvccEntry);
            if (trace)
               log.tracef("Updated context entry %s -> %s", contextEntry, mvccEntry);
         } else {
            // This is a read operation, store the external entry in the context directly.
            ctx.putLookedUpEntry(key, externalEntry);
            if (trace)
               log.tracef("Updated context entry %s -> %s", contextEntry, externalEntry);
         }
      } else {
         if (useRepeatableRead) {
            if (trace) log.tracef("Ignored update %s -> %s as we do repeatable reads", contextEntry, externalEntry);
         } else {
            ctx.putLookedUpEntry(key, externalEntry);
            if (trace) log.tracef("Updated context entry %s -> %s", contextEntry, externalEntry);
         }
      }
   }

   private CacheEntry getFromContext(InvocationContext ctx, Object key) {
      final CacheEntry cacheEntry = ctx.lookupEntry(key);
      if (trace) log.tracef("Exists in context? %s ", cacheEntry);
      return cacheEntry;
   }

   private CacheEntry getFromContainer(Object key, boolean isOwner, boolean writeOperation) {
      if (isOwner) {
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

   private CacheEntry getFromContainerForRead(Object key, boolean isOwner) {
      InternalCacheEntry ice = container.get(key);
      if (trace) {
         log.tracef("Retrieved from container %s", ice);
      }
      if (isOwner) {
         return ice == null ? NullCacheEntry.getInstance() : ice;
      } else {
         return ice == null || !ice.isL1Entry() ? null : ice;
      }
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

   protected MVCCEntry createWrappedEntry(Object key, CacheEntry cacheEntry) {
      Object value = null;
      Metadata metadata = null;
      if (cacheEntry != null) {
         value = cacheEntry.getValue();
         metadata = cacheEntry.getMetadata();
      }

      if (trace) log.tracef("Creating new entry for key %s", toStr(key));
      if (useRepeatableRead) {
         MVCCEntry mvccEntry;
         if (useVersioning) {
            if (metadata == null) {
               metadata = new EmbeddedMetadata.Builder().version(versionGenerator.nonExistingVersion()).build();
            }
            mvccEntry = new VersionedRepeatableReadEntry(key, value, metadata);
         } else {
            mvccEntry = new RepeatableReadEntry(key, value, metadata);
         }
         return mvccEntry;
      } else {
         return new ReadCommittedEntry(key, value, metadata);
      }
   }
}
