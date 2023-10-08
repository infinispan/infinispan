package org.infinispan.distribution.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @author anistor@redhat.com
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
@Scope(Scopes.NAMED_CACHE)
public class DistributionManagerImpl implements DistributionManager {
   private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);

   @Inject Transport transport;
   @Inject KeyPartitioner keyPartitioner;
   @Inject Configuration configuration;

   private CacheMode cacheMode;

   private volatile LocalizedCacheTopology extendedTopology;

   // Start before RpcManagerImpl
   @Start
   @SuppressWarnings("unused")
   void start() throws Exception {
      if (log.isTraceEnabled()) log.tracef("starting distribution manager on %s", getAddress());

      cacheMode = configuration.clustering().cacheMode();
      // We need an extended topology for preload, before the start of StateTransferManagerImpl
      Address localAddress = transport == null ? LocalModeAddress.INSTANCE : transport.getAddress();
      extendedTopology = makeSingletonTopology(cacheMode, keyPartitioner, configuration.clustering().hash().numSegments(),
            localAddress);
   }

   private Address getAddress() {
      return transport.getAddress();
   }

   @Override
   public ConsistentHash getReadConsistentHash() {
      return extendedTopology.getReadConsistentHash();
   }

   @Override
   public ConsistentHash getWriteConsistentHash() {
      return extendedTopology.getWriteConsistentHash();
   }

   @Override
   @ManagedOperation(
         description = "Determines whether a given key is affected by an ongoing rehash, if any.",
         displayName = "Could key be affected by rehash?"
   )
   public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
      if (!isRehashInProgress())
         return false;

      int segment = keyPartitioner.getSegment(key);
      DistributionInfo distributionInfo = this.extendedTopology.getDistribution(segment);
      return distributionInfo.isWriteOwner() && !distributionInfo.isReadOwner();
   }

   /**
    * Tests whether a rehash is in progress
    *
    * @return true if a rehash is in progress, false otherwise
    */
   @Override
   public boolean isRehashInProgress() {
      return extendedTopology.getPendingCH() != null;
   }

   @Override
   public boolean isJoinComplete() {
      return extendedTopology.isConnected();
   }

   @ManagedOperation(
         description = "Tells you whether a given key would be written to this instance of the cache according to the consistent hashing algorithm. " +
               "Only works with String keys.",
         displayName = "Is key local?"
   )
   public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
      return getCacheTopology().isWriteOwner(key);
   }

   @ManagedOperation(
         description = "Shows the addresses of the nodes where a write operation would store the entry associated with the specified key. Only " +
               "works with String keys.",
         displayName = "Locate key"
   )
   public List<String> locateKey(@Parameter(name = "key", description = "Key to locate") String key) {
      List<Address> addresses = getCacheTopology().getDistribution(key).writeOwners();
      return addresses.stream()
                      .map(Address::toString)
                      .collect(Collectors.toList());
   }

   @Override
   public LocalizedCacheTopology getCacheTopology() {
      return this.extendedTopology;
   }

   @Override
   public void setCacheTopology(CacheTopology cacheTopology) {
      if (log.isTraceEnabled()) log.tracef("Topology updated to %s", cacheTopology);
      this.extendedTopology = createLocalizedCacheTopology(cacheTopology);
   }

   @Override
   public LocalizedCacheTopology createLocalizedCacheTopology(CacheTopology cacheTopology) {
      return new LocalizedCacheTopology(cacheMode, cacheTopology, keyPartitioner, transport.getAddress(), true);
   }

   public static LocalizedCacheTopology makeSingletonTopology(CacheMode cacheMode, KeyPartitioner keyPartitioner,
         int numSegments, Address localAddress) {
      List<Address> members = Collections.singletonList(localAddress);
      List<Integer> owners = new ArrayList<>(numSegments);
      for (int i = 0; i < numSegments; i++)
         owners.add(0);
      ConsistentHash ch = new ReplicatedConsistentHash(members, null, Collections.emptyList(), owners);
      CacheTopology cacheTopology = new CacheTopology(-1, -1, ch, null, CacheTopology.Phase.NO_REBALANCE, members, null);
      return new LocalizedCacheTopology(cacheMode, cacheTopology, keyPartitioner, localAddress, false);
   }
}
