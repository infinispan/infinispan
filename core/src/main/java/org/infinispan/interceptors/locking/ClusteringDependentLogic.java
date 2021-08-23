package org.infinispan.interceptors.locking;

import static org.infinispan.transaction.impl.WriteSkewHelper.performWriteSkewCheckAndReturnNewVersions;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.NotifyHelper;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.transaction.impl.WriteSkewHelper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Abstractization for logic related to different clustering modes: replicated or distributed. This implements the <a
 * href="http://en.wikipedia.org/wiki/Bridge_pattern">Bridge</a> pattern as described by the GoF: this plays the role of
 * the <b>Implementor</b> and various LockingInterceptors are the <b>Abstraction</b>.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 */
@Scope(Scopes.NAMED_CACHE)
public interface ClusteringDependentLogic {
   enum Commit {
      /**
       * Do not commit the entry.
       */
      NO_COMMIT(false, false),
      /**
       * Commit the entry but this node is not an owner, therefore, listeners should not be fired.
       */
      COMMIT_NON_LOCAL(true, false),
      /**
       * Commit the entry, this is the owner.
       */
      COMMIT_LOCAL(true, true);

      private final boolean commit;
      private final boolean local;

      Commit(boolean commit, boolean local) {
         this.commit = commit;
         this.local = local;
      }

      public boolean isCommit() {
         return commit;
      }

      public boolean isLocal() {
         return local;
      }
   }

   /**
    * Starts the object - must be first wired via component registry
    */
   void start();

   /**
    * @return information about the location of keys.
    */
   LocalizedCacheTopology getCacheTopology();

   /**
    * Commits the entry to the data container. The commit operation is always done synchronously in the current thread.
    * However notifications for said operations can be performed asynchronously and the returned CompletionStage will
    * complete when the notifications if any are completed.
    * @param entry
    * @param command
    * @param ctx
    * @param trackFlag
    * @param l1Invalidation
    * @return completion stage that is complete when all notifications for the commit are complete or null if already complete
    */
   CompletionStage<Void> commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation);

   /**
    * Determines what type of commit this is. Whether we shouldn't commit, or if this is a commit due to owning the key
    * or not
    * @param command
    * @param ctx
    * @param segment if 0 or greater assumes the underlying container is segmented.
    * @param removed
    * @return
    */
   Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed);


   CompletionStage<Map<Object, IncrementableEntryVersion>> createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand);

   Address getAddress();

   <K, V> EntryLoader<K, V> getEntryLoader();

   @Scope(Scopes.NAMED_CACHE)
   abstract class AbstractClusteringDependentLogic implements ClusteringDependentLogic {
      @Inject protected ComponentRegistry componentRegistry;
      @Inject protected DistributionManager distributionManager;
      @Inject protected InternalDataContainer<Object, Object> dataContainer;
      @Inject protected CacheNotifier<Object, Object> notifier;
      @Inject protected CommitManager commitManager;
      @Inject protected PersistenceManager persistenceManager;
      @Inject protected TimeService timeService;
      @Inject protected FunctionalNotifier<Object, Object> functionalNotifier;
      @Inject protected Configuration configuration;
      @Inject protected KeyPartitioner keyPartitioner;
      @Inject protected EvictionManager<?,?> evictionManager;

      private WriteSkewHelper.KeySpecificLogic keySpecificLogic;
      private EntryLoader<?,?> entryLoader;

      @Start
      public void start() {
         this.keySpecificLogic = initKeySpecificLogic();
         CacheLoaderInterceptor<?,?> cli = componentRegistry.getComponent(CacheLoaderInterceptor.class);
         if (cli != null) {
            entryLoader = cli;
         } else {
            entryLoader = (ctx, key, segment, cmd) -> {
               InternalCacheEntry<Object,Object> ice = dataContainer.peek(segment, key);
               if (ice != null && ice.canExpire() && ice.isExpired(timeService.wallClockTime())) {
                  ice = null;
               }
               return CompletableFuture.completedFuture(ice);
            };
         }
      }

      @Override
      public CompletionStage<Map<Object, IncrementableEntryVersion>> createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         // Perform a write skew check on mapped entries.
         CompletionStage<Map<Object, IncrementableEntryVersion>> uv = performWriteSkewCheckAndReturnNewVersions(prepareCommand, entryLoader, versionGenerator, context,
               keySpecificLogic, keyPartitioner);

         return uv.thenApply(evm -> {
            CacheTransaction cacheTransaction = context.getCacheTransaction();
            Map<Object, IncrementableEntryVersion> uvOld = cacheTransaction.getUpdatedEntryVersions();
            if (uvOld != null && !uvOld.isEmpty()) {
               uvOld.putAll(evm);
               evm = uvOld;
            }
            cacheTransaction.setUpdatedEntryVersions(evm);
            return (evm.isEmpty()) ? null : evm;
         });
      }

      @Override
      public final CompletionStage<Void> commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         if (entry instanceof ClearCacheEntry) {
            return commitClearCommand(dataContainer, ctx, command);
         } else {
            return commitSingleEntry(entry, command, ctx, trackFlag, l1Invalidation);
         }
      }

      private CompletionStage<Void> commitClearCommand(DataContainer<Object, Object> dataContainer, InvocationContext context,
            FlagAffectedCommand command) {
         if (notifier.hasListener(CacheEntryRemoved.class)) {
            Iterator<InternalCacheEntry<Object, Object>> iterator = dataContainer.iteratorIncludingExpired();

            AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            while (iterator.hasNext()) {
               InternalCacheEntry entry = iterator.next();
               // Iterator doesn't support remove
               dataContainer.remove(entry.getKey());
               aggregateCompletionStage.dependsOn(notifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, context, command));
            }
            return aggregateCompletionStage.freeze();
         } else {
            dataContainer.clear();
            return CompletableFutures.completedNull();
         }
      }

      protected abstract CompletionStage<Void> commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                                InvocationContext ctx, Flag trackFlag, boolean l1Invalidation);

      protected Commit clusterCommitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
         // ignore locality for removals, even if skipOwnershipCheck is not true
         if (command != null && command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
            return Commit.COMMIT_LOCAL;
         }

         // Non-transactional caches do not write the entry on the originator when the originator is a backup owner,
         // but that check is done in NonTx/TriangleDistributionInterceptor, so we don't check here again.
         // We also want to allow the command to commit when the originator starts as primary but becomes a backup
         // after the backups acked the write, so the command doesn't have to be retried.
         if (getCacheTopology().isSegmentWriteOwner(segment)) {
            return Commit.COMMIT_LOCAL;
         }
         return Commit.NO_COMMIT;
      }

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
         return clusterCommitType(command, ctx, segment, removed);
      }

      protected abstract WriteSkewHelper.KeySpecificLogic initKeySpecificLogic();

      @Override
      public LocalizedCacheTopology getCacheTopology() {
         return distributionManager.getCacheTopology();
      }

      @Override
      public Address getAddress() {
         return getCacheTopology().getLocalAddress();
      }

      @Override
      public final <K, V> EntryLoader<K, V> getEntryLoader() {
         //noinspection unchecked
         return (EntryLoader<K, V>) entryLoader;
      }
   }

   /**
    * This logic is used in local mode caches.
    */
   class LocalLogic extends AbstractClusteringDependentLogic {
      private LocalizedCacheTopology localTopology;

      @Inject
      public void init(Transport transport, Configuration configuration, KeyPartitioner keyPartitioner) {
         Address address = transport != null ? transport.getAddress() : LocalModeAddress.INSTANCE;
         boolean segmented = configuration.persistence().stores().stream().anyMatch(StoreConfiguration::segmented);
         if (segmented) {
            this.localTopology = LocalizedCacheTopology.makeSegmentedSingletonTopology(keyPartitioner,
                  configuration.clustering().hash().numSegments(), address);
         } else {
            this.localTopology = LocalizedCacheTopology.makeSingletonTopology(CacheMode.LOCAL, address);
         }
      }

      @Override
      public LocalizedCacheTopology getCacheTopology() {
         return localTopology;
      }

      @Override
      public Address getAddress() {
         return localTopology.getLocalAddress();
      }

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
         return Commit.COMMIT_LOCAL;
      }

      @Override
      protected CompletionStage<Void> commitSingleEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx,
                                       Flag trackFlag, boolean l1Invalidation) {
         // Cache flags before they're reset
         // TODO: Can the reset be done after notification instead?
         boolean created = entry.isCreated();
         boolean removed = entry.isRemoved();
         boolean expired = removed && entry instanceof MVCCEntry && ((MVCCEntry) entry).isExpired();

         Object key = entry.getKey();
         int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);

         InternalCacheEntry previousEntry = dataContainer.peek(segment, entry.getKey());
         Object previousValue;
         Metadata previousMetadata;
         if (previousEntry != null) {
            previousValue = previousEntry.getValue();
            previousMetadata = previousEntry.getMetadata();
         } else {
            previousValue = null;
            previousMetadata = null;
         }
         CompletionStage<Void> stage = commitManager.commit(entry, trackFlag, segment, l1Invalidation, ctx);

         // Notify after events if necessary
         return stage.thenCompose(ignore -> NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
               entry, ctx, command, previousValue, previousMetadata, evictionManager));
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic() {
         return WriteSkewHelper.ALWAYS_TRUE_LOGIC;
      }
   }

   /**
    * This logic is used in invalidation mode caches.
    */
   class InvalidationLogic extends AbstractClusteringDependentLogic {

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
         return Commit.COMMIT_LOCAL;
      }

      @Override
      protected CompletionStage<Void> commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                       InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         // Cache flags before they're reset
         // TODO: Can the reset be done after notification instead?
         boolean created = entry.isCreated();
         boolean removed = entry.isRemoved();
         boolean expired;
         if (removed && entry instanceof MVCCEntry) {
            expired = ((MVCCEntry) entry).isExpired();
         } else {
            expired = false;
         }

         Object key = entry.getKey();
         int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);

         InternalCacheEntry previousEntry = dataContainer.peek(segment, entry.getKey());
         Object previousValue;
         Metadata previousMetadata;
         if (previousEntry != null) {
            previousValue = previousEntry.getValue();
            previousMetadata = previousEntry.getMetadata();
         } else {
            previousValue = null;
            previousMetadata = null;
         }
         CompletionStage<Void> stage = commitManager.commit(entry, trackFlag, segment, l1Invalidation, ctx);

         // Notify after events if necessary
         return stage.thenCompose(ignore -> NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
               entry, ctx, command, previousValue, previousMetadata, evictionManager));
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic() {
         return null; //not used because write skew check is not allowed with invalidation
      }
   }

   /**
    * This logic is used in replicated mode caches.
    */
   class ReplicationLogic extends InvalidationLogic {
      @Inject StateTransferLock stateTransferLock;

      private final WriteSkewHelper.KeySpecificLogic localNodeIsPrimaryOwner =
            segment -> getCacheTopology().getSegmentDistribution(segment).isPrimary();

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
         return clusterCommitType(command, ctx, segment, removed);
      }

      @Override
      protected CompletionStage<Void> commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                       InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         Object key = entry.getKey();
         int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);

         Commit doCommit;
         Object previousValue = null;
         Metadata previousMetadata = null;

         CompletionStage<Void> stage = null;
         // Don't allow the CH to change (and state transfer to invalidate entries)
         // between the ownership check and the commit
         stateTransferLock.acquireSharedTopologyLock();
         try {
            doCommit = commitType(command, ctx, segment, entry.isRemoved());
            if (doCommit.isCommit()) {
               InternalCacheEntry previousEntry = dataContainer.peek(segment, key);
               if (previousEntry != null) {
                  previousValue = previousEntry.getValue();
                  previousMetadata = previousEntry.getMetadata();
               }
               stage =commitManager.commit(entry, trackFlag, segment, l1Invalidation, ctx);
            }
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }

         if (doCommit.isCommit() && doCommit.isLocal()) {
            boolean created = entry.isCreated();
            boolean removed = entry.isRemoved();
            boolean expired;
            if (removed && entry instanceof MVCCEntry) {
               expired = ((MVCCEntry) entry).isExpired();
            } else {
               expired = false;
            }

            if (stage == null) {
               return NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
                     entry, ctx, command, previousValue, previousMetadata, evictionManager);
            } else {
               Object finalPreviousValue = previousValue;
               Metadata finalPreviousMetadata = previousMetadata;
               return stage.thenCompose(ignore -> NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
                     entry, ctx, command, finalPreviousValue, finalPreviousMetadata, evictionManager));
            }
         }
         return stage == null ? CompletableFutures.completedNull() : stage;
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic() {
         //in two phase commit, only the primary owner should perform the write skew check
         return localNodeIsPrimaryOwner;
      }
   }

   /**
    * This logic is used in distributed mode caches.
    */
   class DistributionLogic extends AbstractClusteringDependentLogic {
      @Inject StateTransferLock stateTransferLock;

      private final WriteSkewHelper.KeySpecificLogic localNodeIsPrimaryOwner = segment -> getCacheTopology().getSegmentDistribution(segment).isPrimary();

      @Override
      protected CompletionStage<Void> commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                       InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         Object key = entry.getKey();
         int segment = SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);

         Commit doCommit;
         Object previousValue = null;
         Metadata previousMetadata = null;

         // Don't allow the CH to change (and state transfer to invalidate entries)
         // between the ownership check and the commit
         stateTransferLock.acquireSharedTopologyLock();
         CompletionStage<Void> stage = null;
         try {
            doCommit = commitType(command, ctx, segment, entry.isRemoved());

            boolean isL1Write = false;
            if (!doCommit.isCommit() && configuration.clustering().l1().enabled()) {
               // transform for L1
               if (!entry.isRemoved()) {
                  long lifespan = entry.getLifespan();
                  if (lifespan < 0 || lifespan > configuration.clustering().l1().lifespan()) {
                     Metadata metadata = entry.getMetadata().builder()
                                              .lifespan(configuration.clustering().l1().lifespan())
                                              .build();
                     entry.setMetadata(new L1Metadata(metadata));
                  }
               }
               isL1Write = true;
               doCommit = Commit.COMMIT_NON_LOCAL;
            } else if (doCommit.isCommit() && entry.getMetadata() instanceof L1Metadata) {
               throw new IllegalStateException("Local entries must not have L1 metadata");
            }

            if (doCommit.isCommit()) {
               // TODO use value from the entry
               InternalCacheEntry previousEntry = dataContainer.peek(segment, key);
               if (previousEntry != null) {
                  previousValue = previousEntry.getValue();
                  previousMetadata = previousEntry.getMetadata();
               }

               // don't overwrite non-L1 entry with L1 (e.g. when originator == backup
               // and therefore we have two contexts on one node)
               boolean skipL1Write = isL1Write && previousEntry != null && !previousEntry.isL1Entry();
               if (!skipL1Write) {
                  stage = commitManager.commit(entry, trackFlag, segment, l1Invalidation || isL1Write, ctx);
               }
            }
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }

         if (doCommit.isCommit() && doCommit.isLocal()) {
            boolean created = entry.isCreated();
            boolean removed = entry.isRemoved();
            boolean expired;
            if (removed && entry instanceof MVCCEntry) {
               expired = ((MVCCEntry) entry).isExpired();
            } else {
               expired = false;
            }

            if (stage == null || CompletionStages.isCompletedSuccessfully(stage)) {
               return NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
                     entry, ctx, command, previousValue, previousMetadata, evictionManager);
            } else {
               Object finalPreviousValue = previousValue;
               Metadata finalPreviousMetadata = previousMetadata;
               return stage.thenCompose(ignore -> NotifyHelper.entryCommitted(notifier, functionalNotifier, created,
                     removed, expired, entry, ctx, command, finalPreviousValue, finalPreviousMetadata, evictionManager));
            }
         }
         return stage == null ? CompletableFutures.completedNull() : stage;
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic() {
         //in two phase commit, only the primary owner should perform the write skew check
         return localNodeIsPrimaryOwner;
      }
   }

   class ScatteredLogic extends DistributionLogic {
      @Override
      protected CompletableFuture<Void> commitSingleEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         // the logic is in ScatteredDistributionInterceptor
         throw new UnsupportedOperationException();
      }
   }
}
