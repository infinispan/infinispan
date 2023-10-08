package org.infinispan.server.hotrod;

import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_CHECK_ADDRESS_TASK)
class CheckAddressTask implements Function<EmbeddedCacheManager, Boolean> {
   final String cacheName;
   final Address clusterAddress;

   CheckAddressTask(String cacheName, Address clusterAddress) {
      this.cacheName = cacheName;
      this.clusterAddress = clusterAddress;
   }

   @ProtoFactory
   CheckAddressTask(String cacheName, WrappedMessage wrappedAddress) {
      this(cacheName, (Address) WrappedMessages.unwrap(wrappedAddress));
   }

   @ProtoField(1)
   String getCacheName() {
      return cacheName;
   }

   @ProtoField(2)
   WrappedMessage getWrappedAddress() {
      return WrappedMessages.orElseNull(clusterAddress);
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
