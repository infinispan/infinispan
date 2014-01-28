/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.interceptors.locking;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.WriteSkewHelper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.infinispan.transaction.WriteSkewHelper.performWriteSkewCheckAndReturnNewVersions;

/**
 * Abstractization for logic related to different clustering modes: replicated or distributed. This implements the <a
 * href="http://en.wikipedia.org/wiki/Bridge_pattern">Bridge</a> pattern as described by the GoF: this plays the role of
 * the <b>Implementor</b> and various LockingInterceptors are the <b>Abstraction</b>.
 *
 * @author Mircea Markus
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface ClusteringDependentLogic {

   boolean localNodeIsOwner(Object key);

   boolean localNodeIsPrimaryOwner(Object key);

   Address getPrimaryOwner(Object key);

   void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck, InvocationContext ctx);

   Collection<Address> getOwners(Collection<Object> keys);

   EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand);
   
   Address getAddress();

   public static abstract class AbstractClusteringDependentLogic implements ClusteringDependentLogic {

      protected DataContainer dataContainer;

      protected CacheNotifier notifier;
      private ReentrantPerEntryLockContainer lockContainer;

      @Inject
      public void init(DataContainer dataContainer, CacheNotifier notifier, Configuration configuration) {
         this.dataContainer = dataContainer;
         this.notifier = notifier;
         this.lockContainer = createLockContainer(configuration);
      }

      protected void notifyCommitEntry(boolean created, boolean removed,
            boolean evicted, CacheEntry entry, InvocationContext ctx) {
         // Eviction has no notion of pre/post event since 4.2.0.ALPHA4.
         // EvictionManagerImpl.onEntryEviction() triggers both pre and post events
         // with non-null values, so we should do the same here as an ugly workaround.
         if (removed && evicted) {
            notifier.notifyCacheEntryEvicted(
                  entry.getKey(), entry.getValue(), ctx);
         } else if (removed) {
            notifier.notifyCacheEntryRemoved(entry.getKey(), null, false, ctx);
         } else {
            // TODO: We're not very consistent (will JSR-107 solve it?):
            // Current tests expect entry modified to be fired when entry
            // created but not when entry removed

            // Notify entry modified after container has been updated
            notifier.notifyCacheEntryModified(entry.getKey(),
                  entry.getValue(), false, ctx);

            // Notify entry created event after container has been updated
            if (created)
               notifier.notifyCacheEntryCreated(entry.getKey(), false, ctx);
         }
      }

      private ReentrantPerEntryLockContainer createLockContainer(Configuration configuration) {
         //we need a lock container to synchronized the keys being moved between the data container and the persistence
         //also, it needed to merge the DeltaAware values
         return new ReentrantPerEntryLockContainer(configuration.locking().concurrencyLevel());
      }

      public final boolean lock(Object key, boolean noWaitTime) throws InterruptedException {
         if (lockContainer == null) {
            return true;
         }
         final long timeout = noWaitTime ? 0 : 1;
         return lockContainer.acquireLock(null, key, timeout, TimeUnit.DAYS) != null;
      }

      public final void unlock(Object key) {
         if (lockContainer != null) {
            lockContainer.releaseLock(null, key);
         }
      }

      protected final void commitCacheEntry(CacheEntry entry, EntryVersion version) {
         forceLock(entry.getKey());
         entry.commit(dataContainer, version);
         unlock(entry.getKey());
      }

      private void forceLock(Object key) {
         boolean interrupted = false;
         boolean locked = false;
         do {
            try {
               locked = lock(key, false);
            } catch (InterruptedException e) {
               interrupted = true;
            }
         } while (!locked);
         if (interrupted) {
            Thread.currentThread().interrupt();
         }
      }
   }

   /**
    * This logic is used in local mode caches.
    */
   public static class LocalLogic extends AbstractClusteringDependentLogic {

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
      public Collection<Address> getOwners(Collection<Object> keys) {
         return null;
      }

      @Override
      public Address getAddress() {
         return null;
      }

      @Override
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck, InvocationContext ctx) {
         // Cache flags before they're reset
         // TODO: Can the reset be done after notification instead?
         boolean created = entry.isCreated();
         boolean removed = entry.isRemoved();
         boolean evicted = entry.isEvicted();

         entry.commit(dataContainer, newVersion);

         // Notify after events if necessary
         notifyCommitEntry(created, removed, evicted, entry, ctx);
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         throw new IllegalStateException("Cannot invoke this method for local caches");
      }
   }

   /**
    * This logic is used in invalidation mode caches.
    */
   public static class InvalidationLogic extends AbstractClusteringDependentLogic {

      private StateTransferManager stateTransferManager;
      private RpcManager rpcManager;

      private static final WriteSkewHelper.KeySpecificLogic keySpecificLogic = new WriteSkewHelper.KeySpecificLogic() {
         @Override
         public boolean performCheckOnKey(Object key) {
            return true;
         }
      };

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
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck, InvocationContext ctx) {
         // Cache flags before they're reset
         // TODO: Can the reset be done after notification instead?
         boolean created = entry.isCreated();
         boolean removed = entry.isRemoved();
         boolean evicted = entry.isEvicted();

         commitCacheEntry(entry, newVersion);

         // Notify after events if necessary
         notifyCommitEntry(created, removed, evicted, entry, ctx);
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         return null;    //todo [anistor] should I actually return this based on current CH?
      }
      
      @Override
      public Address getAddress() {
         return rpcManager.getAddress();
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         // In REPL mode, this happens if we are the coordinator.
         if (stateTransferManager.getCacheTopology().getReadConsistentHash().getMembers().get(0).equals(rpcManager.getAddress())) {
            // Perform a write skew check on each entry.
            EntryVersionsMap uv = performWriteSkewCheckAndReturnNewVersions(prepareCommand, dataContainer,
                                                                            versionGenerator, context,
                                                                            keySpecificLogic);
            context.getCacheTransaction().setUpdatedEntryVersions(uv);
            return uv;
         } else if (prepareCommand.getModifications().length == 0) {
            // For situations when there's a local-only put in the prepare,
            // simply add an empty entry version map. This works because when
            // a local-only put is executed, this is not added to the prepare
            // modification list.
            context.getCacheTransaction().setUpdatedEntryVersions(new EntryVersionsMap());
         }
         return null;
      }
   }

   /**
    * This logic is used in replicated mode caches.
    */
   public static class ReplicationLogic extends InvalidationLogic {

      private StateTransferLock stateTransferLock;

      @Inject
      public void init(StateTransferLock stateTransferLock) {
         this.stateTransferLock = stateTransferLock;
      }

      @Override
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck, InvocationContext ctx) {
         stateTransferLock.acquireSharedTopologyLock();
         try {
            super.commitEntry(entry, newVersion, skipOwnershipCheck, ctx);
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }
   }

   /**
    * This logic is used in distributed mode caches.
    */
   public static class DistributionLogic extends AbstractClusteringDependentLogic {

      private DistributionManager dm;
      private Configuration configuration;
      private RpcManager rpcManager;
      private StateTransferLock stateTransferLock;

      private final WriteSkewHelper.KeySpecificLogic keySpecificLogic = new WriteSkewHelper.KeySpecificLogic() {
         @Override
         public boolean performCheckOnKey(Object key) {
            return localNodeIsOwner(key);
         }
      };

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
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck, InvocationContext ctx) {
         // Don't allow the CH to change (and state transfer to invalidate entries)
         // between the ownership check and the commit
         stateTransferLock.acquireSharedTopologyLock();
         try {
            boolean doCommit = true;
            // ignore locality for removals, even if skipOwnershipCheck is not true
            boolean isForeignOwned = !skipOwnershipCheck && !localNodeIsOwner(entry.getKey());
            if (isForeignOwned && !entry.isRemoved()) {
               if (configuration.clustering().l1().enabled()) {
                  // transform for L1
                  if (entry.getLifespan() < 0 || entry.getLifespan() > configuration.clustering().l1().lifespan())
                     entry.setLifespan(configuration.clustering().l1().lifespan());
               } else {
                  doCommit = false;
               }
            }

            boolean created = false;
            boolean removed = false;
            boolean evicted = false;
            if (!isForeignOwned) {
               created = entry.isCreated();
               removed = entry.isRemoved();
               evicted = entry.isEvicted();
            }

            if (doCommit)
               commitCacheEntry(entry, newVersion);
            else
               entry.rollback();

            if (!isForeignOwned) {
               notifyCommitEntry(created, removed, evicted, entry, ctx);
            }
         } finally {
            stateTransferLock.releaseSharedTopologyLock();
         }
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         return dm.getAffectedNodes(keys);
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         // Perform a write skew check on mapped entries.
         EntryVersionsMap uv = performWriteSkewCheckAndReturnNewVersions(prepareCommand, dataContainer,
                                                                         versionGenerator, context,
                                                                         keySpecificLogic);

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
}
