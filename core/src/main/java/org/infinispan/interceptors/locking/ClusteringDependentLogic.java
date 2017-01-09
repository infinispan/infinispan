package org.infinispan.interceptors.locking;

import static org.infinispan.transaction.impl.WriteSkewHelper.performTotalOrderWriteSkewCheckAndReturnNewVersions;
import static org.infinispan.transaction.impl.WriteSkewHelper.performWriteSkewCheckAndReturnNewVersions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.Immutables;
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
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.L1Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
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
 * @deprecated Since 9.0, no longer public API.
 */
@Deprecated
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

   boolean localNodeIsOwner(Object key);

   boolean localNodeIsPrimaryOwner(Object key);

   Address getPrimaryOwner(Object key);

   void commitEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx,
                    Flag trackFlag, boolean l1Invalidation);

   Commit commitType(FlagAffectedCommand command, InvocationContext ctx, Object key, boolean removed);

   List<Address> getOwners(Collection<Object> keys);

   List<Address> getOwners(Object key);

   EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand);

   Address getAddress();

   int getSegmentForKey(Object key);

   abstract class AbstractClusteringDependentLogic implements ClusteringDependentLogic {

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
                       FunctionalNotifier<Object, Object> functionalNotifier) {
         this.dataContainer = dataContainer;
         this.notifier = notifier;
         this.totalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
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
      public final void commitEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         if (entry instanceof ClearCacheEntry) {
            //noinspection unchecked
            commitClearCommand(dataContainer, (ClearCacheEntry<Object, Object>) entry, ctx, command);
         } else {
            commitSingleEntry(entry, metadata, command, ctx, trackFlag, l1Invalidation);
         }
      }

      private void commitClearCommand(DataContainer<Object, Object> dataContainer, ClearCacheEntry<Object, Object> cacheEntry,
                                      InvocationContext context, FlagAffectedCommand command) {
         List<InternalCacheEntry<Object, Object>> copyEntries = new ArrayList<>(dataContainer.entrySet());
         cacheEntry.commit(dataContainer, null);
         for (InternalCacheEntry entry : copyEntries) {
            notifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, context, command);
         }
      }

      protected abstract void commitSingleEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command,
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
            if (localNodeIsOwner(key)) {
               return Commit.COMMIT_LOCAL;
            } else if (removed) {
               return Commit.COMMIT_NON_LOCAL;
            }
         } else {
            // in non-tx mode, on backup we don't commit in original context, backup command has its own context.
            return localNodeIsPrimaryOwner(key) ? Commit.COMMIT_LOCAL : Commit.NO_COMMIT;
         }
         return Commit.NO_COMMIT;
      }

      protected abstract WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder);

      protected void notifyCommitEntry(boolean created, boolean removed, boolean expired, CacheEntry entry,
              InvocationContext ctx, FlagAffectedCommand command, Object previousValue, Metadata previousMetadata) {
         boolean isWriteOnly = (command instanceof WriteCommand) && ((WriteCommand) command).isWriteOnly();
         if (removed) {
            if (command instanceof RemoveCommand) {
               ((RemoveCommand) command).notify(ctx, previousValue, previousMetadata, false);
            } else if (command instanceof InvalidateCommand) {
               notifier.notifyCacheEntryInvalidated(entry.getKey(), entry.getValue(), entry.getMetadata(), false, ctx, command);
            } else {
               if (expired) {
                  notifier.notifyCacheEntryExpired(entry.getKey(), previousValue, previousMetadata, ctx);
               } else {
                  notifier.notifyCacheEntryRemoved(entry.getKey(), previousValue, previousMetadata, false, ctx, command);
               }

               // A write-only command only writes and so can't 100% guarantee
               // to be able to retrieve previous value when removed, so only
               // send remove event when the command is read-write.
               if (!isWriteOnly)
                  functionalNotifier.notifyOnRemove(EntryViews.readOnly(entry.getKey(), previousValue, previousMetadata));

               functionalNotifier.notifyOnWrite(() -> EntryViews.noValue(entry.getKey()));
            }
         } else {
            // Notify entry event after container has been updated
            if (created) {
               notifier.notifyCacheEntryCreated(
                     entry.getKey(), entry.getValue(), entry.getMetadata(), false, ctx, command);

               // A write-only command only writes and so can't 100% guarantee
               // that an entry has been created, so only send create event
               // when the command is read-write.
               if (!isWriteOnly)
                  functionalNotifier.notifyOnCreate(EntryViews.readOnly(entry));

               functionalNotifier.notifyOnWrite(() -> EntryViews.readOnly(entry));
            } else {
               notifier.notifyCacheEntryModified(entry.getKey(), entry.getValue(), entry.getMetadata(), previousValue,
                     previousMetadata, false, ctx, command);

               // A write-only command only writes and so can't 100% guarantee
               // that an entry has been created, so only send modify when the
               // command is read-write.
               if (!isWriteOnly)
                  functionalNotifier.notifyOnModify(
                     EntryViews.readOnly(entry.getKey(), previousValue, previousMetadata),
                     EntryViews.readOnly(entry));

               functionalNotifier.notifyOnWrite(() -> EntryViews.readOnly(entry));
            }
         }
      }

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
   }

   /**
    * This logic is used in local mode caches.
    */
   class LocalLogic extends AbstractClusteringDependentLogic {

      private EmbeddedCacheManager cacheManager;

      @Inject
      public void init(EmbeddedCacheManager cacheManager) {
         this.cacheManager = cacheManager;
      }

      @Override
      public boolean localNodeIsOwner(Object key) {
         return true;
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         return true;
      }

      @Override
      public Address getPrimaryOwner(Object key) {
         throw new IllegalStateException("Cannot invoke this method for local caches");
      }

      @Override
      public List<Address> getOwners(Collection<Object> keys) {
         return null;
      }

      @Override
      public List<Address> getOwners(Object key) {
         return null;
      }

      @Override
      public Address getAddress() {
         Address address = cacheManager.getAddress();
         if (address == null) {
            address = LocalModeAddress.INSTANCE;
         }
         return address;
      }

      @Override
      public int getSegmentForKey(Object key) {
         return 0;
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx,
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
         commitManager.commit(entry, metadata, trackFlag, l1Invalidation);

         // Notify after events if necessary
         notifyCommitEntry(created, removed, expired, entry, ctx, command, previousValue, previousMetadata);
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         throw new IllegalStateException("Cannot invoke this method for local caches");
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return null; //not used
      }
   }

   /**
    * This logic is used in invalidation mode caches.
    */
   class InvalidationLogic extends AbstractClusteringDependentLogic {

      private StateTransferManager stateTransferManager;
      private RpcManager rpcManager;

      @Inject
      public void init(RpcManager rpcManager, StateTransferManager stateTransferManager) {
         this.rpcManager = rpcManager;
         this.stateTransferManager = stateTransferManager;
      }

      @Override
      public boolean localNodeIsOwner(Object key) {
         CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         return cacheTopology == null || cacheTopology.getWriteConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key);
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         CacheTopology cacheTopology = stateTransferManager.getCacheTopology();
         return cacheTopology == null || cacheTopology.getWriteConsistentHash().locatePrimaryOwner(key).equals(rpcManager.getAddress());
      }

      @Override
      public Address getPrimaryOwner(Object key) {
         return stateTransferManager.getCacheTopology().getWriteConsistentHash().locatePrimaryOwner(key);
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command,
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
         commitManager.commit(entry, metadata, trackFlag, l1Invalidation);

         // Notify after events if necessary
         notifyCommitEntry(created, removed, expired, entry, ctx, command, previousValue, previousMetadata);
      }

      @Override
      public List<Address> getOwners(Collection<Object> keys) {
         return null;    //todo [anistor] should I actually return this based on current CH?
      }

      @Override
      public List<Address> getOwners(Object key) {
         return null;
      }

      @Override
      public Address getAddress() {
         return rpcManager.getAddress();
      }

      @Override
      public int getSegmentForKey(Object key) {
         return stateTransferManager.getCacheTopology().getWriteConsistentHash().getSegment(key);
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

      private final WriteSkewHelper.KeySpecificLogic localNodeIsPrimaryOwner = this::localNodeIsPrimaryOwner;

      @Inject
      public void init(StateTransferLock stateTransferLock) {
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command,
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
               commitManager.commit(entry, metadata, trackFlag, l1Invalidation);
               if (doCommit.isLocal()) {
                  notifyCommitEntry(created, removed, expired, entry, ctx, command, previousValue, previousMetadata);
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
      private DistributionManager dm;
      private Configuration configuration;
      private RpcManager rpcManager;
      private StateTransferLock stateTransferLock;

      private final WriteSkewHelper.KeySpecificLogic localNodeIsOwner = this::localNodeIsOwner;
      private final WriteSkewHelper.KeySpecificLogic localNodeIsPrimaryOwner = this::localNodeIsPrimaryOwner;

      @Inject
      public void init(DistributionManager dm, Configuration configuration,
                       RpcManager rpcManager, StateTransferLock stateTransferLock) {
         this.dm = dm;
         this.configuration = configuration;
         this.rpcManager = rpcManager;
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      public boolean localNodeIsOwner(Object key) {
         return dm.getLocality(key).isLocal();
      }

      @Override
      public Address getAddress() {
         return rpcManager.getAddress();
      }

      @Override
      public int getSegmentForKey(Object key) {
         return dm.getWriteConsistentHash().getSegment(key);
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         final Address address = rpcManager.getAddress();
         return dm.getPrimaryLocation(key).equals(address);
      }

      @Override
      public Address getPrimaryOwner(Object key) {
         return dm.getPrimaryLocation(key);
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command,
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
                  long lifespan;
                  if (metadata != null) {
                     lifespan = metadata.lifespan();
                  } else {
                     lifespan = entry.getLifespan();
                  }
                  if (lifespan < 0 || lifespan > configuration.clustering().l1().lifespan()) {
                     Metadata.Builder builder;
                     if (metadata != null) {
                        builder = metadata.builder();
                     } else {
                        builder = entry.getMetadata().builder();
                     }
                     metadata = builder
                        .lifespan(configuration.clustering().l1().lifespan())
                        .build();
                     metadata = new L1Metadata(metadata);
                  }
               }
               isL1Write = true;
               doCommit = Commit.COMMIT_NON_LOCAL;
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
                  commitManager.commit(entry, metadata, trackFlag, l1Invalidation);
                  if (doCommit.isLocal()) {
                     notifyCommitEntry(created, removed, expired, entry, ctx, command, previousValue, previousMetadata);
                  }
               }
            }
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }

      @Override
      public List<Address> getOwners(Collection<Object> affectedKeys) {
         if (affectedKeys.isEmpty()) {
            return Collections.emptyList();
         }
         return Immutables.immutableListConvert(dm.locateAll(affectedKeys));
      }

      @Override
      public List<Address> getOwners(Object key) {
         return Immutables.immutableListConvert(dm.locate(key));
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
}
