package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_MORTAL_CACHE_VALUE_RESPONSE)
public class SuccessfulMortalCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulMortalCacheValueResponse(MortalCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public MortalCacheValue getResponseValue() {
      return (MortalCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulMortalCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
