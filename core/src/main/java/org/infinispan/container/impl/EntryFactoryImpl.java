package org.infinispan.container.impl;

import static org.infinispan.commons.util.Util.toStr;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.entries.ReadCommittedEntry;
import org.infinispan.container.entries.RepeatableReadEntry;
import org.infinispan.container.entries.VersionedRepeatableReadEntry;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link EntryFactory} implementation to be used for optimistic locking scheme.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public class EntryFactoryImpl implements EntryFactory {

   private static final Log log = LogFactory.getLog(EntryFactoryImpl.class);

   @Inject InternalDataContainer container;
   @Inject Configuration configuration;
   @Inject TimeService timeService;
   @Inject VersionGenerator versionGenerator;
   @Inject DistributionManager distributionManager;
   @Inject InternalExpirationManager expirationManager;

   private boolean isL1Enabled;
   private boolean useRepeatableRead;
   private boolean useVersioning;
   private PrivateMetadata nonExistingPrivateMetadata;

   @Start (priority = 8)
   public void init() {
      // Scattered mode needs repeatable-read entries to properly retry half-committed multi-key operations
      // (see RetryingEntryWrappingInterceptor for details).
      useRepeatableRead = configuration.transaction().transactionMode().isTransactional()
            && configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ;
      isL1Enabled = configuration.clustering().l1().enabled();
      // Write-skew check implies isolation level = REPEATABLE_READ && locking mode = OPTIMISTIC
      useVersioning = Configurations.isTxVersioned(configuration);
      nonExistingPrivateMetadata = new PrivateMetadata.Builder()
            .entryVersion(versionGenerator.nonExistingVersion())
            .build();
   }

   @Override
   public final CompletionStage<Void> wrapEntryForReading(InvocationContext ctx, Object key, int segment, boolean isOwner,
                                                          boolean hasLock, CompletionStage<Void> previousStage) {
      if (!isOwner && !isL1Enabled) {
         return previousStage;
      }
      CacheEntry cacheEntry = getFromContext(ctx, key);
      if (cacheEntry == null) {
         InternalCacheEntry readEntry = getFromContainer(key, segment);
         if (readEntry == null) {
            if (isOwner) {
               addReadEntryToContext(ctx, NullCacheEntry.getInstance(), key);
            }
         } else if (isOwner || readEntry.isL1Entry()) {
            if (readEntry.canExpire()) {
               CompletionStage<Boolean> expiredStage = expirationManager.handlePossibleExpiration(readEntry, segment, hasLock);
               if (CompletionStages.isCompletedSuccessfully(expiredStage)) {
                  Boolean expired = CompletionStages.join(expiredStage);
                  handleExpiredEntryContextAddition(expired, ctx, readEntry, key, isOwner);
               } else {
                  return expiredStage.thenAcceptBoth(previousStage, (expired, __) -> {
                     handleExpiredEntryContextAddition(expired, ctx, readEntry, key, isOwner);
                  });
               }
            } else {
               addReadEntryToContext(ctx, readEntry, key);
            }
         }
      }

      return previousStage;
   }

   private void handleExpiredEntryContextAddition(Boolean expired, InvocationContext ctx, InternalCacheEntry readEntry,
         Object key, boolean isOwner) {
      // Multi-key commands perform the expiration check in parallel, so they need synchronization
      if (expired == Boolean.FALSE) {
         addReadEntryToContext(ctx, readEntry, key);
      } else if (isOwner) {
         addReadEntryToContext(ctx, NullCacheEntry.getInstance(), key);
      }
   }

   private void addReadEntryToContext(InvocationContext ctx, CacheEntry cacheEntry, Object key) {
      // With repeatable read, we need to create a RepeatableReadEntry as internal cache entries are mutable
      // Otherwise we can store the InternalCacheEntry directly in the context
      if (useRepeatableRead) {
         MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry);
         mvccEntry.setRead();
         cacheEntry = mvccEntry;
      }
      if (log.isTraceEnabled()) {
         log.tracef("Wrap %s for read. Entry=%s", toStr(key), cacheEntry);
      }
      ctx.putLookedUpEntry(key, cacheEntry);
   }

   private void addWriteEntryToContext(InvocationContext ctx, CacheEntry cacheEntry, Object key, boolean isRead) {
      MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry);
      if (cacheEntry.isNull()) {
         mvccEntry.setCreated(true);
      }
      if (isRead) {
         mvccEntry.setRead();
      }
      ctx.putLookedUpEntry(key, mvccEntry);
      if (log.isTraceEnabled())
         log.tracef("Added context entry %s", mvccEntry);
   }

   @Override
   public CompletionStage<Void> wrapEntryForWriting(InvocationContext ctx, Object key, int segment, boolean isOwner,
                                                    boolean isRead, CompletionStage<Void> previousStage) {
      CacheEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         // Nothing to do, already wrapped.
      } else if (contextEntry != null) {
         // Already in the context as an InternalCacheEntry
         // Need to wrap it in a MVCCEntry.
         MVCCEntry mvccEntry = createWrappedEntry(key, contextEntry);
         ctx.putLookedUpEntry(key, mvccEntry);
         if (log.isTraceEnabled())
            log.tracef("Updated context entry %s -> %s", contextEntry, mvccEntry);
      } else {
         // Not in the context yet.
         InternalCacheEntry ice = getFromContainer(key, segment);
         if (isOwner) {
            if (ice == null) {
               addWriteEntryToContext(ctx, NullCacheEntry.getInstance(), key, isRead);
            } else {
               if (ice.canExpire()) {
                  CompletionStage<Boolean> expiredStage = expirationManager.handlePossibleExpiration(ice, segment, true);
                  if (CompletionStages.isCompletedSuccessfully(expiredStage)) {
                     Boolean expired = CompletionStages.join(expiredStage);
                     handleWriteExpiredEntryContextAddition(expired, ctx, ice, key, isRead);
                  } else {
                     // Serialize invocation context access
                     return expiredStage.thenAcceptBoth(previousStage, (expired, __) -> {
                        handleWriteExpiredEntryContextAddition(expired, ctx, ice, key, isRead);
                     });
                  }
               } else {
                  addWriteEntryToContext(ctx, ice, key, isRead);
               }
            }
         } else if (isL1Enabled && ice != null && !ice.isL1Entry()) {
            addWriteEntryToContext(ctx, ice, key, isRead);
         }
      }

      return previousStage;
   }

   private void handleWriteExpiredEntryContextAddition(Boolean expired, InvocationContext ctx, InternalCacheEntry ice,
         Object key, boolean isRead) {
      // Multi-key commands perform the expiration check in parallel, so they need synchronization
      if (expired == Boolean.FALSE) {
         addWriteEntryToContext(ctx, ice, key, isRead);
      } else {
         addWriteEntryToContext(ctx, NullCacheEntry.getInstance(), key, isRead);
      }
   }

   @Override
   public void wrapEntryForWritingSkipExpiration(InvocationContext ctx, Object key, int segment, boolean isOwner) {
      CacheEntry contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         // Nothing to do, already wrapped.
      } else if (contextEntry != null) {
         // Already in the context as an InternalCacheEntry
         // Need to wrap it in a MVCCEntry.
         MVCCEntry mvccEntry = createWrappedEntry(key, contextEntry);
         ctx.putLookedUpEntry(key, mvccEntry);
         if (log.isTraceEnabled())
            log.tracef("Updated context entry %s -> %s", contextEntry, mvccEntry);
      } else if (isOwner) {
         // Not in the context yet.
         CacheEntry cacheEntry = getFromContainer(key, segment);
         if (cacheEntry == null) {
            cacheEntry = NullCacheEntry.getInstance();
         }
         MVCCEntry mvccEntry = createWrappedEntry(key, cacheEntry);
         // Make sure to set the created date so we can verify if the entry actually expired
         mvccEntry.setCreated(cacheEntry.getCreated());
         if (cacheEntry.isNull()) {
            mvccEntry.setCreated(true);
         }
         mvccEntry.setRead();
         ctx.putLookedUpEntry(key, mvccEntry);
         if (log.isTraceEnabled())
            log.tracef("Updated context entry null -> %s", mvccEntry);
      }
   }

   @Override
   public void wrapExternalEntry(InvocationContext ctx, Object key, CacheEntry externalEntry, boolean isRead,
         boolean isWrite) {
      // For a write operation, the entry is always already wrapped. For a read operation, the entry may be
      // in the context as an InternalCacheEntry, as null, or missing altogether.
      CacheEntry<?, ?> contextEntry = getFromContext(ctx, key);
      if (contextEntry instanceof MVCCEntry) {
         MVCCEntry mvccEntry = (MVCCEntry) contextEntry;
         // Already wrapped for a write. Update the value and the metadata.
         if (mvccEntry.skipLookup()) {
            // This can happen during getGroup() invocations, which request the whole group from remote nodes
            // even if some keys are already in the context.
            if (log.isTraceEnabled())
               log.tracef("Ignored update for context entry %s", contextEntry);
            return;
         }
         // Without updating initial value a local write skew check would fail when the entry is loaded
         // from the cache store. This shouldn't be called more than once since afterwards we set skipLookup
         mvccEntry.setValue(externalEntry.getValue());
         mvccEntry.setCreated(externalEntry.getCreated());
         mvccEntry.setLastUsed(externalEntry.getLastUsed());
         mvccEntry.setMetadata(externalEntry.getMetadata());
         mvccEntry.setInternalMetadata(externalEntry.getInternalMetadata());
         mvccEntry.updatePreviousValue();
         if (log.isTraceEnabled()) log.tracef("Updated context entry %s", contextEntry);
      } else if (contextEntry == null || contextEntry.isNull()) {
         if (isWrite || useRepeatableRead) {
            MVCCEntry<?, ?> mvccEntry = createWrappedEntry(key, externalEntry);
            if (isRead) {
               mvccEntry.setRead();
            }
            ctx.putLookedUpEntry(key, mvccEntry);
            if (log.isTraceEnabled())
               log.tracef("Updated context entry %s -> %s", contextEntry, mvccEntry);
         } else {
            // This is a read operation, store the external entry in the context directly.
            ctx.putLookedUpEntry(key, externalEntry);
            if (log.isTraceEnabled())
               log.tracef("Updated context entry %s -> %s", contextEntry, externalEntry);
         }
      } else {
         if (useRepeatableRead) {
            if (log.isTraceEnabled()) log.tracef("Ignored update %s -> %s as we do repeatable reads", contextEntry, externalEntry);
         } else {
            ctx.putLookedUpEntry(key, externalEntry);
            if (log.isTraceEnabled()) log.tracef("Updated context entry %s -> %s", contextEntry, externalEntry);
         }
      }
   }

   private CacheEntry<?, ?> getFromContext(InvocationContext ctx, Object key) {
      final CacheEntry<?, ?> cacheEntry = ctx.lookupEntry(key);
      if (log.isTraceEnabled()) log.tracef("Exists in context? %s ", cacheEntry);
      return cacheEntry;
   }

   private boolean isPrimaryOwner(int segment) {
      return distributionManager == null ||
            distributionManager.getCacheTopology().getSegmentDistribution(segment).isPrimary();
   }

   private InternalCacheEntry getFromContainer(Object key, int segment) {
      InternalCacheEntry ice = container.peek(segment, key);
      if (log.isTraceEnabled()) {
         log.tracef("Retrieved from container %s", ice);
      }
      return ice;
   }

   protected MVCCEntry<?, ?> createWrappedEntry(Object key, CacheEntry<?, ?> cacheEntry) {
      Object value = null;
      Metadata metadata = null;
      PrivateMetadata internalMetadata = null;
      if (cacheEntry != null) {
         synchronized (cacheEntry) {
            value = cacheEntry.getValue();
            metadata = cacheEntry.getMetadata();
            internalMetadata = cacheEntry.getInternalMetadata();
         }
      }

      if (log.isTraceEnabled()) log.tracef("Creating new entry for key %s", toStr(key));
      MVCCEntry<?, ?> mvccEntry;
      if (useRepeatableRead) {
         if (useVersioning) {
            if (internalMetadata == null) {
               internalMetadata = nonExistingPrivateMetadata;
            }
            mvccEntry = new VersionedRepeatableReadEntry(key, value, metadata);
         } else {
            mvccEntry = new RepeatableReadEntry(key, value, metadata);
         }
      } else {
         mvccEntry = new ReadCommittedEntry(key, value, metadata);
      }
      mvccEntry.setInternalMetadata(internalMetadata);
      if (cacheEntry != null) {
         mvccEntry.setCreated(cacheEntry.getCreated());
         mvccEntry.setLastUsed(cacheEntry.getLastUsed());
      }
      return mvccEntry;
   }
}
