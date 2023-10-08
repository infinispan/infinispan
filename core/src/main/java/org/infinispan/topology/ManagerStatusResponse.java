package org.infinispan.topology;

import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
* @author Dan Berindei
* @since 7.1
*/
@ProtoTypeId(ProtoStreamTypeIds.MANAGER_STATUS_RESPONSE)
public class ManagerStatusResponse {

   @ProtoField(number = 1)
   final MarshallableMap<String, CacheStatusResponse> caches;

   @ProtoField(number = 2, defaultValue = "false")
   final boolean rebalancingEnabled;

   @ProtoFactory
   ManagerStatusResponse(MarshallableMap<String, CacheStatusResponse> caches, boolean rebalancingEnabled) {
      this.caches = caches;
      this.rebalancingEnabled = rebalancingEnabled;
   }

   public ManagerStatusResponse(Map<String, CacheStatusResponse> caches, boolean rebalancingEnabled) {
      this(MarshallableMap.create(caches), rebalancingEnabled);
   }

   public Map<String, CacheStatusResponse> getCaches() {
      return MarshallableMap.unwrap(caches);
   }

   public boolean isRebalancingEnabled() {
      return rebalancingEnabled;
   }

   @Override
   public String toString() {
      return "ManagerStatusResponse{" +
            "caches=" + getCaches() +
            ", rebalancingEnabled=" + rebalancingEnabled +
            '}';
   }
}
