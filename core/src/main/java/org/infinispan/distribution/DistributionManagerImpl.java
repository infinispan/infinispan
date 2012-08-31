/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Immutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @author Dan Berindei <dan@infinispan.org>
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
public class DistributionManagerImpl implements DistributionManager {
   private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // Injected components
   private Configuration configuration;
   private RpcManager rpcManager;
   private CommandsFactory cf;
   private CacheNotifier cacheNotifier;
   private StateTransferManager stateTransferManager;

   private volatile CacheTopology cacheTopology;
   private GlobalConfiguration globalCfg;

   /**
    * Default constructor
    */
   public DistributionManagerImpl() {
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory cf, CacheNotifier cacheNotifier,
                    StateTransferManager stateTransferManager, GlobalConfiguration globalCfg) {
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.cf = cf;
      this.cacheNotifier = cacheNotifier;
      this.stateTransferManager = stateTransferManager;
      this.globalCfg = globalCfg;
   }

   // The DMI is cache-scoped, so it will always start after the RMI, which is global-scoped
   @Start(priority = 20)
   private void start() throws Exception {
      if (trace) log.tracef("starting distribution manager on %s", getAddress());
   }

   private int getReplCount() {
      return configuration.clustering().hash().numOwners();
   }

   private Address getAddress() {
      return rpcManager.getAddress();
   }

   @Override
   @Deprecated
   public boolean isLocal(Object key) {
      return getLocality(key).isLocal();
   }

   @Override
   public DataLocality getLocality(Object key) {
      boolean local = isKeyLocalToAddress(key);
      if (isRehashInProgress()) {
         if (local) {
            return DataLocality.LOCAL_UNCERTAIN;
         } else {
            return DataLocality.NOT_LOCAL_UNCERTAIN;
         }
      } else {
         if (local) {
            return DataLocality.LOCAL;
         } else {
            return DataLocality.NOT_LOCAL;
         }
      }
   }

   private boolean isKeyLocalToAddress(Object key) {
      // TODO Add a boolean flag to select the read consistent hash?
      return cacheTopology.getWriteConsistentHash().isKeyLocalToNode(getAddress(), key);
   }

   @Override
   public List<Address> locate(Object key) {
      return getConsistentHash().locateOwners(key);
   }

   @Override
   public Address getPrimaryLocation(Object key) {
      return getConsistentHash().locatePrimaryOwner(key);
   }

   @Override
   public Set<Address> locateAll(Collection<Object> keys) {
      return getConsistentHash().locateAllOwners(keys);
   }

   @Override
   public void transformForL1(CacheEntry entry) {
      if (entry.getLifespan() < 0 || entry.getLifespan() > configuration.clustering().l1().lifespan())
         entry.setLifespan(configuration.clustering().l1().lifespan());
   }

   @Override
   public InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock) throws Exception {
      GlobalTransaction gtx = acquireRemoteLock ? ((TxInvocationContext)ctx).getGlobalTransaction() : null;
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, ctx.getFlags(), acquireRemoteLock, gtx);

      List<Address> targets = locate(key);
      // if any of the recipients has left the cluster since the command was issued, just don't wait for its response
      targets.retainAll(rpcManager.getTransport().getMembers());
      ResponseFilter filter = new ClusteredGetResponseValidityFilter(targets, getAddress());
      Map<Address, Response> responses = rpcManager.invokeRemotely(targets, get, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                                                   configuration.clustering().sync().replTimeout(), true, filter);

      if (!responses.isEmpty()) {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
               return cacheValue.toInternalCacheEntry(key);
            }
         }
      }

      return null;
   }

   @Override
   public ConsistentHash getConsistentHash() {
      return cacheTopology.getWriteConsistentHash();
   }

   @Override
   public ConsistentHash setCacheTopology(CacheTopology newCacheTopology) {
      if (trace) log.tracef("Installing new cache topology %s", newCacheTopology);
      // TODO Replace the topology change notification with another notification that accepts two consistent hashes and a topology id
      ConsistentHash oldCH = cacheTopology.getWriteConsistentHash();
      ConsistentHash newCH = newCacheTopology.getWriteConsistentHash();
      cacheNotifier.notifyTopologyChanged(oldCH, newCH, true);
      this.cacheTopology = newCacheTopology;
      cacheNotifier.notifyTopologyChanged(oldCH, newCH, false);
      return oldCH;
   }


   // TODO Move these methods to the StateTransferManager interface so we can eliminate the dependency
   @Override
   @ManagedOperation(description = "Determines whether a given key is affected by an ongoing rehash, if any.")
   @Operation(displayName = "Could key be affected by rehash?")
   public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
      return stateTransferManager.isLocationInDoubt(key);
   }

   /**
    * Tests whether a rehash is in progress
    * @return true if a rehash is in progress, false otherwise
    */
   @Override
   public boolean isRehashInProgress() {
      return stateTransferManager.isStateTransferInProgress();
   }

   @Override
   public boolean isJoinComplete() {
      return stateTransferManager.isJoinComplete();
   }

   @Override
   public Collection<Address> getAffectedNodes(Collection<Object> affectedKeys) {
      if (affectedKeys == null || affectedKeys.isEmpty()) {
         if (trace) log.trace("affected keys are empty");
         return Collections.emptyList();
      }

      Set<Address> an = locateAll(affectedKeys);
      return Immutables.immutableListConvert(an);
   }

   @ManagedOperation(description = "Tells you whether a given key is local to this instance of the cache.  Only works with String keys.")
   @Operation(displayName = "Is key local?")
   public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
      return getLocality(key).isLocal();
   }

   @ManagedOperation(description = "Locates an object in a cluster.  Only works with String keys.")
   @Operation(displayName = "Locate key")
   public List<String> locateKey(@Parameter(name = "key", description = "Key to locate") String key) {
      List<String> l = new LinkedList<String>();
      for (Address a : locate(key)) l.add(a.toString());
      return l;
   }

   @Override
   public String toString() {
      return "DistributionManagerImpl[" + cacheTopology + "]";
   }
}
