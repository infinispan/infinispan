package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_METADATA_IMMORTAL_CACHE_VALUE_RESPONSE)
public class SuccessfulMetadataImmortalMortalCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulMetadataImmortalMortalCacheValueResponse(MetadataImmortalCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public MetadataImmortalCacheValue getResponseValue() {
      return (MetadataImmortalCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulMetadataImmortalMortalCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
