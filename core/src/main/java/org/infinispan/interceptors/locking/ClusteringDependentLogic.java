package org.infinispan.interceptors.locking;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.InfinispanCollections;
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
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.FunctionalNotifier;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.CommitManager;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.impl.WriteSkewHelper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.TimeService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.infinispan.transaction.impl.WriteSkewHelper.performTotalOrderWriteSkewCheckAndReturnNewVersions;
import static org.infinispan.transaction.impl.WriteSkewHelper.performWriteSkewCheckAndReturnNewVersions;

/**
 * Abstractization for logic related to different clustering modes: replicated or distributed. This implements the <a
 * href="http://en.wikipedia.org/wiki/Bridge_pattern">Bridge</a> pattern as described by the GoF: this plays the role of
 * the <b>Implementor</b> and various LockingInterceptors are the <b>Abstraction</b>.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface ClusteringDependentLogic {

   boolean localNodeIsOwner(Object key);

   boolean localNodeIsPrimaryOwner(Object key);

   Address getPrimaryOwner(Object key);

   void commitEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx,
                    Flag trackFlag, boolean l1Invalidation);

   List<Address> getOwners(Collection<Object> keys);

   List<Address> getOwners(Object key);

   EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand);

   Address getAddress();

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

      protected abstract WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder);

      protected void notifyCommitEntry(boolean created, boolean removed, boolean expired, CacheEntry entry,
              InvocationContext ctx, FlagAffectedCommand command, Object previousValue, Metadata previousMetadata) {
         boolean isWriteOnly = (command instanceof WriteCommand) && ((WriteCommand) command).isWriteOnly();
         if (removed) {
            if (command instanceof RemoveCommand) {
               ((RemoveCommand)command).notify(ctx, previousValue, previousMetadata, false);
            } else {
               if (expired) {
                  notifier.notifyCacheEntryExpired(entry.getKey(), previousValue, previousMetadata, ctx);
               } else {
                  notifier.notifyCacheEntryRemoved(
                          entry.getKey(), previousValue, previousMetadata, false, ctx, command);
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
               notifier.notifyCacheEntryModified(entry.getKey(), entry.getValue(), entry.getMetadata(), previousValue, previousMetadata,
                                                 false, ctx, command);

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
            address = LOCAL_MODE_ADDRESS;
         }
         return address;
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

   Address LOCAL_MODE_ADDRESS = new Address() {
      @Override
      public String toString() {
         return "Local Address";
      }

      @Override
      public int compareTo(Address o) {
         return 0;
      }
   };

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
         return stateTransferManager.getCacheTopology().getWriteConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key);
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         return stateTransferManager.getCacheTopology().getWriteConsistentHash().locatePrimaryOwner(key).equals(rpcManager.getAddress());
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
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return null; //not used because write skew check is not allowed with invalidation
      }
   }

   /**
    * This logic is used in replicated mode caches.
    */
   class ReplicationLogic extends InvalidationLogic {

      private StateTransferLock stateTransferLock;

      @Inject
      public void init(StateTransferLock stateTransferLock) {
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      protected void commitSingleEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command,
                                       InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
         stateTransferLock.acquireSharedTopologyLock();
         try {
            super.commitSingleEntry(entry, metadata, command, ctx, trackFlag, l1Invalidation);
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return totalOrder ?
               new WriteSkewHelper.KeySpecificLogic() {
                  @Override
                  public boolean performCheckOnKey(Object key) {
                     //in total order, all nodes perform the write skew check
                     return true;
                  }
               } :
               new WriteSkewHelper.KeySpecificLogic() {
                  @Override
                  public boolean performCheckOnKey(Object key) {
                     //in two phase commit, only the primary owner should perform the write skew check
                     return localNodeIsPrimaryOwner(key);
                  }
               };
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
            boolean doCommit = true;
            // ignore locality for removals, even if skipOwnershipCheck is not true
            boolean skipOwnershipCheck = command != null &&
                  command.hasFlag(Flag.SKIP_OWNERSHIP_CHECK);

            boolean isForeignOwned = !skipOwnershipCheck && !localNodeIsOwner(entry.getKey());
            if (isForeignOwned && !entry.isRemoved()) {
               if (configuration.clustering().l1().enabled()) {
                  // transform for L1
                  metadata = getL1Metadata(entry, metadata);
               } else {
                  doCommit = false;
               }
            }

            boolean created = false;
            boolean removed = false;
            boolean expired = false;
            if (!isForeignOwned) {
               created = entry.isCreated();
               removed = entry.isRemoved();
               if (removed && entry instanceof MVCCEntry) {
                  expired = ((MVCCEntry) entry).isExpired();
               }
            }

            if (doCommit) {
               InternalCacheEntry previousEntry = dataContainer.peek(entry.getKey());
               Object previousValue = null;
               Metadata previousMetadata = null;
               if (previousEntry != null) {
                  previousValue = previousEntry.getValue();
                  previousMetadata = previousEntry.getMetadata();
               }
               commitManager.commit(entry, metadata, trackFlag, l1Invalidation);
               if (!isForeignOwned) {
                  notifyCommitEntry(created, removed, expired, entry, ctx, command, previousValue, previousMetadata);
               }
            } else
               entry.rollback();

         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }

      private Metadata getL1Metadata(CacheEntry entry, Metadata metadata) {
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
         }
         return metadata;
      }

      @Override
      public List<Address> getOwners(Collection<Object> affectedKeys) {
         if (affectedKeys.isEmpty()) {
            return InfinispanCollections.emptyList();
         }
         return Immutables.immutableListConvert(dm.locateAll(affectedKeys));
      }

      @Override
      public List<Address> getOwners(Object key) {
         return Immutables.immutableListConvert(dm.locate(key));
      }

      @Override
      protected WriteSkewHelper.KeySpecificLogic initKeySpecificLogic(boolean totalOrder) {
         return totalOrder ?
               new WriteSkewHelper.KeySpecificLogic() {
                  @Override
                  public boolean performCheckOnKey(Object key) {
                     //in total order, all the owners can perform the write skew check.
                     return localNodeIsOwner(key);
                  }
               } :
               new WriteSkewHelper.KeySpecificLogic() {
                  @Override
                  public boolean performCheckOnKey(Object key) {
                     //in two phase commit, only the primary owner should perform the write skew check
                     return localNodeIsPrimaryOwner(key);
                  }
               };
      }
   }
}
