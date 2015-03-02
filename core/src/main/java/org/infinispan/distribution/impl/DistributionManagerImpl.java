package org.infinispan.distribution.impl;

import org.infinispan.distribution.DataLocality;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LookupMode;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @author Dan Berindei <dan@infinispan.org>
 * @author anistor@redhat.com
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
public class DistributionManagerImpl implements DistributionManager {

   private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   // Injected components
   private RpcManager rpcManager;
   private StateTransferManager stateTransferManager;

   /**
    * Default constructor
    */
   public DistributionManagerImpl() {
   }

   @Inject
   public void init(RpcManager rpcManager, StateTransferManager stateTransferManager) {
      this.rpcManager = rpcManager;
      this.stateTransferManager = stateTransferManager;
   }

   // The DMI is cache-scoped, so it will always start after the RMI, which is global-scoped
   @Start(priority = 20)
   @SuppressWarnings("unused")
   private void start() throws Exception {
      if (trace) log.tracef("starting distribution manager on %s", getAddress());
   }

   private Address getAddress() {
      return rpcManager.getAddress();
   }

   @Override
   public DataLocality getLocality(Object key, LookupMode lookupMode) {
      assertNonNull(lookupMode);

      final boolean transferInProgress = isAffectedByRehash(key);
      final CacheTopology topology = stateTransferManager.getCacheTopology();

      // Null topology means state transfer has not occurred,
      // hence data should be stored locally.
      final boolean local = topology == null
            || lookupMode.getConsistentHash(topology).isKeyLocalToNode(getAddress(), key);

      if (transferInProgress) {
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

   @Override
   public List<Address> locate(Object key, LookupMode lookupMode) {
      return getConsistentHashFor(lookupMode).locateOwners(key);
   }

   @Override
   public Address getPrimaryLocation(Object key, LookupMode lookupMode) {
      return getConsistentHashFor(lookupMode).locatePrimaryOwner(key);
   }

   @Override
   public Set<Address> locateAll(Collection<Object> keys, LookupMode lookupMode) {
      return getConsistentHashFor(lookupMode).locateAllOwners(keys);
   }

   @Override
   public ConsistentHash getReadConsistentHash() {
      return stateTransferManager.getCacheTopology().getReadConsistentHash();
   }

   @Override
   public ConsistentHash getWriteConsistentHash() {
      return stateTransferManager.getCacheTopology().getWriteConsistentHash();
   }

   // TODO Move these methods to the StateTransferManager interface so we can eliminate the dependency
   @Override
   @ManagedOperation(
         description = "Determines whether a given key is affected by an ongoing rehash, if any.",
         displayName = "Could key be affected by rehash?"
   )
   public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
      return stateTransferManager.isStateTransferInProgressForKey(key);
   }

   /**
    * Tests whether a rehash is in progress
    *
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

   @ManagedOperation(
         description = "Tells you whether a given key is local to this instance of the cache according to the consistent hashing algorithm. " +
               "Only works with String keys. This operation might return true even if the object does not exist in the cache.",
         displayName = "Is key local?"
   )
   public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
      return getLocality(key, LookupMode.READ).isLocal() || getLocality(key, LookupMode.WRITE).isLocal();
   }

   @ManagedOperation(
         description = "Shows the addresses of the nodes where a put operation would store the entry associated with the specified key. Only " +
               "works with String keys. The list of potential owners is returned even if the object does not exist in the cache.",
         displayName = "Locate key"
   )
   public List<String> locateKey(@Parameter(name = "key", description = "Key to locate") String key) {
      List<String> ownerList = new LinkedList<>();
      for (Address owner : locate(key, LookupMode.WRITE)) {
         ownerList.add(String.valueOf(owner));
      }
      return ownerList;
   }

   private ConsistentHash getConsistentHashFor(LookupMode lookupMode) {
      assertNonNull(lookupMode);
      return lookupMode.getConsistentHash(stateTransferManager.getCacheTopology());
   }

   private static void assertNonNull(LookupMode lookupMode) {
      if (lookupMode == null) {
         throw new NullPointerException("LookupMode can't be null.");
      }
   }
}
