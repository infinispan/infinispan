package org.infinispan.server.hotrod;

import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_CHECK_ADDRESS_TASK)
class CheckAddressTask implements Function<EmbeddedCacheManager, Boolean> {
   final String cacheName;
   final Address clusterAddress;

   CheckAddressTask(String cacheName, Address clusterAddress) {
      this.cacheName = cacheName;
      this.clusterAddress = clusterAddress;
   }

   @ProtoFactory
   CheckAddressTask(String cacheName, JGroupsAddress address) {
      this(cacheName, (Address) address);
   }

   @ProtoField(1)
   String getCacheName() {
      return cacheName;
   }

   @ProtoField(number = 2, javaType = JGroupsAddress.class)
   Address getAddress() {
      return clusterAddress;
   }

   @Override
   public Boolean apply(EmbeddedCacheManager embeddedCacheManager) {
      if (embeddedCacheManager.isRunning(cacheName)) {
         Cache<Address, ServerAddress> cache = embeddedCacheManager.getCache(cacheName);
         return cache.containsKey(clusterAddress);
      }
      // If the cache isn't started just play like this node has the address in the cache - it will be added as it
      // joins, so no worries
      return true;
   }
}
