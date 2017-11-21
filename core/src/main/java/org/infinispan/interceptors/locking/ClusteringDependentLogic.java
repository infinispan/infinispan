package org.infinispan.interceptors.locking;

import static org.infinispan.transaction.impl.WriteSkewHelper.performTotalOrderWriteSkewCheckAndReturnNewVersions;
import static org.infinispan.transaction.impl.WriteSkewHelper.performWriteSkewCheckAndReturnNewVersions;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.NotifyHelper;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.transaction.impl.WriteSkewHelper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.TimeService;

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

      private boolean commit;
      private boolean local;

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
    * @return information about the location of keys.
    */
   LocalizedCacheTopology getCacheTopology();

   /**
    * @deprecated Since 9.0, please use {@code getCacheTopology().isWriteOwner(key)} instead.
    */
   @Deprecated
   default boolean localNodeIsOwner(Object key) {
      return getCacheTopology().isWriteOwner(key);
   }

   /**
    * @deprecated Since 9.0, please use {@code getCacheTopology().getDistribution(key).isPrimary()} instead.
    */
   @Deprecated
   default boolean localNodeIsPrimaryOwner(Object key) {
      return getCacheTopology().getDistribution(key).isPrimary();
   }

   /**
    * @deprecated Since 9.0, please use {@code getCacheTopology().getDistributionInfo(key).primary()} instead.
    */
   @Deprecated
   default Address getPrimaryOwner(Object key) {
      return getCacheTopology().getDistribution(key).primary();
   }

   void commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation);

   Commit commitType(FlagAffectedCommand command, InvocationContext ctx, Object key, boolean removed);

   /**
    * @deprecated Since 9.0, please use {@code getCacheTopology().getWriteOwners(keys)} instead.
    */
   @Deprecated
   default Collection<Address> getOwners(Collection<Object> keys) {
      return getCacheTopology().getWriteOwners(keys);
   }

   /**
    * @deprecated Since 9.0, please use {@code getCacheTopology().getWriteOwners(key)} instead.
    */
   @Deprecated
   default Collection<Address> getOwners(Object key) {
      return getCacheTopology().getWriteOwners(key);
   }


   EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand);

   Address getAddress();

   abstract class AbstractClusteringDependentLogic implements ClusteringDependentLogic {
      protected DistributionManager distributionManager;
      protected DataContainer<Object, Object> dataContainer;
      protected CacheNotifier<Object, Object> notifier;
      protected boolean totalOrder;
      private WriteSkewHelper.KeySpecificLogic keySpecificLogic;
      protected CommitManager commitManager;
      protected PersistenceManager persistenceManager;
      protected TimeService timeService;
      protected FunctionalNotifier<Object, Object> functionalNotifier;

      @Inject
      public void init(DataContainer<Object, Object> dataContainer, CacheNotifier<Object, Object> notifier, Configuration configuration,
                       CommitManager commitManager, PersistenceManager persistenceManager, TimeService timeService,
                       FunctionalNotifier<Object, Object> functionalNotifier, DistributionManager distributionManager) {
         this.dataContainer = dataContainer;
         this.notifier = notifier;
         this.totalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
         this.distributionManager = distributionManager;
         this.keySpecificLogic = initKeySpecificLogic(totalOrder);
         this.commitManager = commitManager;
         this.persistenceManager = persistenceManager;
         this.timeService = timeService;
         this.functionalNotifier = functionalNotifier;
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         return totalOrder ?
               totalOrderCreateNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand) :
               clusteredCreateNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand);
      }

      @Override
      public final void commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         if (entry instanceof ClearCacheEntry) {
            //noinspection unchecked
            commitClearCommand(dataContainer, (ClearCacheEntry<Object, Object>) entry, ctx, command);
         } else {
            commitSingleEntry(entry, command, ctx, trackFlag, l1Invalidation);
         }
      }

      private void commitClearCommand(DataContainer<Object, Object> dataContainer, ClearCacheEntry<Object, Object> cacheEntry,
                                      InvocationContext context, FlagAffectedCommand command) {
         Iterator<InternalCacheEntry<Object, Object>> iterator = dataContainer.iterator();

         while (iterator.hasNext()) {
            InternalCacheEntry entry = iterator.next();
            // Iterator doesn't support remove
            dataContainer.remove(entry.getKey());
            notifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, context, command);
         }
      }

      protected abstract void commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                                InvocationContext ctx, Flag trackFlag, boolean l1Invalidation);

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, Object key, boolean removed) {
         // ignore locality for removals, even if skipOwnershipCheck is not true
         if (command != null && command.hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK)) {
            return Commit.COMMIT_LOCAL;
         }
         boolean transactional = ctx.isInTxScope() && (command == null || !command.hasAnyFlag(FlagBitSets.PUT_FOR_EXTERNAL_READ));
         // When a command is local-mode, it does not get written by replicating origin -> primary -> backup but
         // when origin == backup it's written right from the original context
         // ClearCommand is also broadcast to all nodes from originator, and on originator it should remove entries
         // for which this node is backup owner even though it did not get the removal from primary owner.
         if (transactional || !ctx.isOriginLocal() || (command != null &&
               (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL) || command instanceof ClearCommand))) {
            // During ST, entries whose ownership is lost are invalidated by InvalidateCommand
            // and at that point we're no longer owners - the only information is that the origin
            // is local and the entry is removed.
            if (getCacheTopology().isWriteOwner(key)) {
               return Commit.COMMIT_LOCAL;
            } else if (removed) {
               return Commit.COMMIT_NON_LOCAL;
            }
         } else {
            // in non-tx mode, on backup we don't commit in original context, backup command has its own context.
            return getCacheTopology().getDistribution(key).isPrimary() ? Commit.COMMIT_LOCAL : Commit.NO_COMMIT;
         }
         return Commit.NO_COMMIT;
      }

      protected abstract WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder);

      private EntryVersionsMap totalOrderCreateNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context,
                                                                                VersionedPrepareCommand prepareCommand) {
         if (context.isOriginLocal()) {
            throw new IllegalStateException("This must not be reached");
         }

         EntryVersionsMap updatedVersionMap = new EntryVersionsMap();

         if (!((TotalOrderPrepareCommand) prepareCommand).skipWriteSkewCheck()) {
            updatedVersionMap = performTotalOrderWriteSkewCheckAndReturnNewVersions(prepareCommand, dataContainer,
                                                                                    persistenceManager, versionGenerator,
                                                                                    context, keySpecificLogic, timeService);
         }

         for (WriteCommand c : prepareCommand.getModifications()) {
            for (Object k : c.getAffectedKeys()) {
               if (keySpecificLogic.performCheckOnKey(k)) {
                  if (!updatedVersionMap.containsKey(k)) {
                     updatedVersionMap.put(k, null);
                  }
               }
            }
         }

         context.getCacheTransaction().setUpdatedEntryVersions(updatedVersionMap);
         return updatedVersionMap;
      }

      private EntryVersionsMap clusteredCreateNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context,
                                                                               VersionedPrepareCommand prepareCommand) {
         // Perform a write skew check on mapped entries.
         EntryVersionsMap uv = performWriteSkewCheckAndReturnNewVersions(prepareCommand, dataContainer,
                                                                         persistenceManager, versionGenerator, context,
                                                                         keySpecificLogic, timeService);

         CacheTransaction cacheTransaction = context.getCacheTransaction();
         EntryVersionsMap uvOld = cacheTransaction.getUpdatedEntryVersions();
         if (uvOld != null && !uvOld.isEmpty()) {
            uvOld.putAll(uv);
            uv = uvOld;
         }
         cacheTransaction.setUpdatedEntryVersions(uv);
         return (uv.isEmpty()) ? null : uv;
      }

      @Override
      public LocalizedCacheTopology getCacheTopology() {
         return distributionManager.getCacheTopology();
      }

      @Override
      public Address getAddress() {
         return getCacheTopology().getLocalAddress();
      }
   }

   /**
    * This logic is used in local mode caches.
    */
   class LocalLogic extends AbstractClusteringDependentLogic {
      private LocalizedCacheTopology localTopology;

      @Inject
      public void init(Transport transport) {
         Address address = transport != null ? transport.getAddress() : LocalModeAddress.INSTANCE;
         this.localTopology = LocalizedCacheTopology.makeSingletonTopology(CacheMode.LOCAL, address);
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
      protected void commitSingleEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx,
                                       Flag trackFlag, boolean l1Invalidation) {
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

         InternalCacheEntry previousEntry = dataContainer.peek(entry.getKey());
         Object previousValue = null;
         Metadata previousMetadata = null;
         if (previousEntry != null) {
            previousValue = previousEntry.getValue();
            previousMetadata = previousEntry.getMetadata();
         }
         commitManager.commit(entry, trackFlag, l1Invalidation, ctx);

         // Notify after events if necessary
         NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
               entry, ctx, command, previousValue, previousMetadata);
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return key -> true;
      }
   }

   /**
    * This logic is used in invalidation mode caches.
    */
   class InvalidationLogic extends AbstractClusteringDependentLogic {

      @Override
      protected void commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
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

         InternalCacheEntry previousEntry = dataContainer.peek(entry.getKey());
         Object previousValue = null;
         Metadata previousMetadata = null;
         if (previousEntry != null) {
            previousValue = previousEntry.getValue();
            previousMetadata = previousEntry.getMetadata();
         }
         commitManager.commit(entry, trackFlag, l1Invalidation, ctx);

         // Notify after events if necessary
         NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
               entry, ctx, command, previousValue, previousMetadata);
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return null; //not used because write skew check is not allowed with invalidation
      }
   }

   /**
    * This logic is used in replicated mode caches.
    */
   class ReplicationLogic extends InvalidationLogic {
      private StateTransferLock stateTransferLock;

      private final WriteSkewHelper.KeySpecificLogic localNodeIsPrimaryOwner =
            (key) -> getCacheTopology().getDistribution(key).isPrimary();

      @Inject
      public void init(StateTransferLock stateTransferLock) {
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      public Collection<Address> getOwners(Object key) {
         return null;
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         if (keys.isEmpty())
            return Collections.emptyList();

         return null;
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                       InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         // Don't allow the CH to change (and state transfer to invalidate entries)
         // between the ownership check and the commit
         stateTransferLock.acquireSharedTopologyLock();
         try {
            Commit doCommit = commitType(command, ctx, entry.getKey(), entry.isRemoved());
            if (doCommit.isCommit()) {
               boolean created = false;
               boolean removed = false;
               boolean expired = false;
               if (doCommit.isLocal()) {
                  created = entry.isCreated();
                  removed = entry.isRemoved();
                  if (removed && entry instanceof MVCCEntry) {
                     expired = ((MVCCEntry) entry).isExpired();
                  }
               }

               InternalCacheEntry previousEntry = dataContainer.peek(entry.getKey());
               Object previousValue = null;
               Metadata previousMetadata = null;
               if (previousEntry != null) {
                  previousValue = previousEntry.getValue();
                  previousMetadata = previousEntry.getMetadata();
               }
               commitManager.commit(entry, trackFlag, l1Invalidation, ctx);
               if (doCommit.isLocal()) {
                  NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
                        entry, ctx, command, previousValue, previousMetadata);
               }
            }
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return totalOrder
               //in total order, all nodes perform the write skew check
               ? key -> true
               //in two phase commit, only the primary owner should perform the write skew check
               : localNodeIsPrimaryOwner;
      }
   }

   /**
    * This logic is used in distributed mode caches.
    */
   class DistributionLogic extends AbstractClusteringDependentLogic {
      private Configuration configuration;
      private StateTransferLock stateTransferLock;

      private final WriteSkewHelper.KeySpecificLogic localNodeIsOwner = (key) -> getCacheTopology().isWriteOwner(key);
      private final WriteSkewHelper.KeySpecificLogic localNodeIsPrimaryOwner =
            (key) -> getCacheTopology().getDistribution(key).isPrimary();

      @Inject
      public void init(Configuration configuration, StateTransferLock stateTransferLock) {
         this.configuration = configuration;
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, FlagAffectedCommand command,
                                       InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         // Don't allow the CH to change (and state transfer to invalidate entries)
         // between the ownership check and the commit
         stateTransferLock.acquireSharedTopologyLock();
         try {
            Commit doCommit = commitType(command, ctx, entry.getKey(), entry.isRemoved());

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
               boolean created = false;
               boolean removed = false;
               boolean expired = false;
               if (doCommit.isLocal()) {
                  created = entry.isCreated();
                  removed = entry.isRemoved();
                  if (removed && entry instanceof MVCCEntry) {
                     expired = ((MVCCEntry) entry).isExpired();
                  }
               }

               // TODO use value from the entry
               InternalCacheEntry previousEntry = dataContainer.peek(entry.getKey());
               Object previousValue = null;
               Metadata previousMetadata = null;
               if (previousEntry != null) {
                  previousValue = previousEntry.getValue();
                  previousMetadata = previousEntry.getMetadata();
               }
               if (isL1Write && previousEntry != null && !previousEntry.isL1Entry()) {
                  // don't overwrite non-L1 entry with L1 (e.g. when originator == backup
                  // and therefore we have two contexts on one node)
               } else {
                  commitManager.commit(entry, trackFlag, l1Invalidation || isL1Write, ctx);
                  if (doCommit.isLocal()) {
                     NotifyHelper.entryCommitted(notifier, functionalNotifier, created, removed, expired,
                           entry, ctx, command, previousValue, previousMetadata);
                  }
               }
            }
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return totalOrder
               //in total order, all the owners can perform the write skew check.
               ? localNodeIsOwner
               //in two phase commit, only the primary owner should perform the write skew check
               : localNodeIsPrimaryOwner;
      }
   }

   class ScatteredLogic extends DistributionLogic {
      @Override
      protected void commitSingleEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         // the logic is in ScatteredDistributionInterceptor
         throw new UnsupportedOperationException();
      }
   }
}
