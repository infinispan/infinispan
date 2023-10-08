package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_TRANSIENT_CACHE_VALUE_RESPONSE)
public class SuccessfulTransientCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulTransientCacheValueResponse(TransientCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public TransientCacheValue getResponseValue() {
      return (TransientCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulTransientCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
