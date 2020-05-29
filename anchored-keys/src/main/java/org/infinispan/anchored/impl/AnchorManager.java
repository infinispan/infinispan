package org.infinispan.anchored.impl;

import static org.infinispan.factories.scopes.Scopes.NAMED_CACHE;

import java.util.List;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.remoting.transport.Address;

/**
 * Holds the current writer in an anchored cache.
 *
 * @author Dan Berindei
 * @since 11
 */
@Scope(NAMED_CACHE)
public class AnchorManager {
   @Inject DistributionManager distributionManager;
//   @Inject CacheNotifier cacheNotifier;

//   private volatile Address currentWriter;
//   @GuardedBy("this")
//   int currentTopologyId = -1;
//
//   @Start
//   public void start() {
//      cacheNotifier.addListener(this);
//      updateWriter(distributionManager.getCacheTopology());
//   }
//
//   @TopologyChanged
//   public void onTopologyChange(TopologyChangedEvent<?, ?> event) {
//      updateWriter(distributionManager.getCacheTopology());
//   }
//
//   private void updateWriter(CacheTopology cacheTopology) {
//      synchronized (this) {
//         if (cacheTopology.getTopologyId() > currentTopologyId) {
//            currentTopologyId = cacheTopology.getTopologyId();
//            List<Address> readMembers = cacheTopology.getReadConsistentHash().getMembers();
//            currentWriter = (readMembers.get(readMembers.size() - 1));
//         }
//      }
//   }

   public Address getCurrentWriter() {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      List<Address> members = cacheTopology.getReadConsistentHash().getMembers();
      if (members.size() == 1)
         return null;

      Address newestMember = members.get(members.size() - 1);
      if (newestMember.equals(cacheTopology.getLocalAddress()))
         return null;

      return newestMember;
   }

   public boolean isCurrentWriter() {
      return getCurrentWriter() == null;
   }

   public Address updateLocation(Address address) {
      if (address != null && distributionManager.getCacheTopology().getMembersSet().contains(address)) {
         return address;
      }
      return getCurrentWriter();
   }
}
