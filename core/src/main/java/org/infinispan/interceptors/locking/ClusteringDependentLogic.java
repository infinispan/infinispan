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

import org.infinispan.CacheException;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.versioning.*;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.WriteSkewHelper;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;

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

   static final Log log = LogFactory.getLog(ClusteringDependentLogic.class);

   boolean localNodeIsOwner(Object key);

   boolean localNodeIsPrimaryOwner(Object key);

   void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck);

   Collection<Address> getOwners(Collection<Object> keys);

   EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand);

   Address getAddress();


   /**
    * This logic is used when a changing a key affects all the nodes in the cluster, e.g. int the replicated,
    * invalidated and local cache modes.
    */
   public static final class AllNodesLogic implements ClusteringDependentLogic {

      private DataContainer dataContainer;

      private RpcManager rpcManager;
      private static final WriteSkewHelper.KeySpecificLogic keySpecificLogic = new WriteSkewHelper.KeySpecificLogic() {
         @Override
         public boolean performCheckOnKey(Object key) {
            return true;
         }
      };


      @Inject
      public void init(DataContainer dc, RpcManager rpcManager) {
         this.dataContainer = dc;
         this.rpcManager = rpcManager;
      }

      @Override
      public boolean localNodeIsOwner(Object key) {
         return true;
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         return rpcManager == null || rpcManager.getTransport().isCoordinator();
      }

      @Override
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
         entry.commit(dataContainer, newVersion);
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         return null;
      }

      @Override
      public Address getAddress() {
         return rpcManager.getAddress();
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         // In REPL mode, this happens if we are the coordinator.
         if (rpcManager.getTransport().isCoordinator()) {
            // Perform a write skew check on each entry.
            EntryVersionsMap uv = performWriteSkewCheckAndReturnNewVersions(prepareCommand, dataContainer,
                  versionGenerator, context,
                  keySpecificLogic);
            context.getCacheTransaction().setUpdatedEntryVersions(uv);
            return uv;
         }
         return null;
      }
   }

   public static final class DistributionLogic implements ClusteringDependentLogic {

      private DistributionManager dm;
      private DataContainer dataContainer;
      private Configuration configuration;
      private RpcManager rpcManager;
      private final WriteSkewHelper.KeySpecificLogic keySpecificLogic = new WriteSkewHelper.KeySpecificLogic() {
         @Override
         public boolean performCheckOnKey(Object key) {
            return localNodeIsOwner(key);
         }
      };

      @Inject
      public void init(DistributionManager dm, DataContainer dataContainer, Configuration configuration, RpcManager rpcManager) {
         this.dm = dm;
         this.dataContainer = dataContainer;
         this.configuration = configuration;
         this.rpcManager = rpcManager;
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
         final boolean result = dm.getPrimaryLocation(key).equals(address);
         log.tracef("My address is %s. Am I main owner? - %b", address, result);
         return result;
      }

      @Override
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
         boolean doCommit = true;
         // ignore locality for removals, even if skipOwnershipCheck is not true
         if (!skipOwnershipCheck && !entry.isRemoved() && !localNodeIsOwner(entry.getKey())) {
            if (configuration.isL1CacheEnabled()) {
               dm.transformForL1(entry);
            } else {
               doCommit = false;
            }
         }
         if (doCommit)
            entry.commit(dataContainer, newVersion);
         else
            entry.rollback();
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

   //Pedro -- Logic for total order protocol in replicated mode
   public static final class TotalOrderAllNodesLogic implements ClusteringDependentLogic {

      private DataContainer dataContainer;
      private RpcManager rpcManager;

      @Inject
      public void init(DataContainer dc, RpcManager rpcManager) {
         this.dataContainer = dc;
         this.rpcManager = rpcManager;
      }


      @Override
      public boolean localNodeIsOwner(Object key) {
         return true;
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         //no lock acquisition in total order
         return false;
      }

      @Override
      public void commitEntry(CacheEntry entry, EntryVersion newVersion, boolean skipOwnershipCheck) {
         entry.commit(dataContainer, newVersion);
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         return null;
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator,
                                                                     TxInvocationContext context,
                                                                     VersionedPrepareCommand prepareCommand) {
         if (context.isOriginLocal()) {
            throw new IllegalStateException("This must not be reached");
         }

         EntryVersionsMap updatedVersionMap = new EntryVersionsMap();
         EntryVersionsMap versionsSeenMap = prepareCommand.getVersionsSeen();

         for (Object key : prepareCommand.getAffectedKeys()) {
            ClusteredRepeatableReadEntry entry = (ClusteredRepeatableReadEntry) context.lookupEntry(key);

            //save the original version and updates to the version that transaction reads
            IncrementableEntryVersion originalLocalVersion = (IncrementableEntryVersion) entry.getVersion();
            entry.setVersion(versionsSeenMap.get(key));

            if (context.hasFlag(Flag.SKIP_WRITE_SKEW_CHECK) ||
                  !entry.isMarkedForWriteSkew() ||
                  entry.performWriteSkewCheck(dataContainer)) {
               IncrementableEntryVersion newVersion = createNewVersion(originalLocalVersion,
                     (IncrementableEntryVersion) entry.getVersion(),
                     versionGenerator);

               updatedVersionMap.put(key, newVersion);
            } else {
               // Write skew check detected!
               throw new CacheException("Write skew detected on key " + key + " for transaction " +
                     Util.prettyPrintGlobalTransaction(prepareCommand.getGlobalTransaction()));
            }
         }

         context.getCacheTransaction().setUpdatedEntryVersions(updatedVersionMap);
         return updatedVersionMap;
      }

      @Override
      public Address getAddress() {
         return rpcManager.getAddress();
      }

      private IncrementableEntryVersion createNewVersion(IncrementableEntryVersion originalLocalVersion,
                                                         IncrementableEntryVersion versionSeen, VersionGenerator versionGenerator) {
         if (originalLocalVersion == null) {
            if (versionSeen == null) {
               //both are null (new entry)
               return versionGenerator.generateNew();
            } else {
               //version seen is not null... increment that one
               return versionGenerator.increment(versionSeen);
            }
         } else {
            if (versionSeen == null) {
               //original version is not null
               return versionGenerator.increment(originalLocalVersion);
            } else {
               //both are not null. choose the greatest
               if (originalLocalVersion.compareTo(versionSeen) == InequalVersionComparisonResult.AFTER) {
                  return versionGenerator.increment(originalLocalVersion);
               } else {
                  return versionGenerator.increment(versionSeen);
               }
            }
         }
      }
   }
}
