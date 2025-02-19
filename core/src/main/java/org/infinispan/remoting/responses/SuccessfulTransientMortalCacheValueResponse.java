package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_TRANSIENT_MORTAL_CACHE_VALUE_RESPONSE)
public class SuccessfulTransientMortalCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulTransientMortalCacheValueResponse(TransientMortalCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public TransientMortalCacheValue getResponseValue() {
      return (TransientMortalCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulTransientMortalCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
