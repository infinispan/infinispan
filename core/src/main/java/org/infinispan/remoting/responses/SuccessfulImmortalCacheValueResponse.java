package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_IMMORTAL_CACHE_VALUE_RESPONSE)
public class SuccessfulImmortalCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulImmortalCacheValueResponse(ImmortalCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public ImmortalCacheValue getResponseValue() {
      return (ImmortalCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulImmortalCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
