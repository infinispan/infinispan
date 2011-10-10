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

import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;

/**
 * Abstractization for logic related to different clustering modes: replicated or distributed.
 * This implements the <a href="http://en.wikipedia.org/wiki/Bridge_pattern">Bridge</a> pattern as described by the GoF:
 * this plays the role of the <b>Implementor</b> and various LockingInterceptors are the <b>Abstraction</b>.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public interface ClusteringDependentLogic {

   static final Log log = LogFactory.getLog(ClusteringDependentLogic.class);

   boolean localNodeIsOwner(Object key);

   boolean localNodeIsPrimaryOwner(Object key);

   void commitEntry(CacheEntry entry, boolean skipOwnershipCheck);

   Collection<Address> getOwners(Collection<Object> keys);

   /**
    * This logic is used when a changing a key affects all the nodes in the cluster, e.g. int the replicated,
    * invalidated and local cache modes.
    */
   public static final class AllNodesLogic implements ClusteringDependentLogic {

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
         return rpcManager == null || rpcManager.getTransport().isCoordinator();
      }

      @Override
      public void commitEntry(CacheEntry entry, boolean skipOwnershipCheck) {
         entry.commit(dataContainer);
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         return null;
      }
   }

   public static final class DistributionLogic implements ClusteringDependentLogic {

      private DistributionManager dm;
      private DataContainer dataContainer;
      private Configuration configuration;
      private RpcManager rpcManager;

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
      public boolean localNodeIsPrimaryOwner(Object key) {
         final List<Address> locate = dm.locate(key);
         final Address address = rpcManager.getAddress();
         final boolean result = locate.get(0).equals(address);
         log.tracef("Node owners are %s and my address is %s. Am I main owner? - %b", locate, address, result);
         return result;
      }

      @Override
      public void commitEntry(CacheEntry entry, boolean skipOwnershipCheck) {
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
            entry.commit(dataContainer);
         else
            entry.rollback();
      }

      @Override
      public Collection<Address> getOwners(Collection<Object> keys) {
         return dm.getAffectedNodes(keys);
      }
   }
}
