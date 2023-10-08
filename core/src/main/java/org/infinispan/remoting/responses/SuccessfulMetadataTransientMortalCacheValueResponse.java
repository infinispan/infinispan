package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_METADATA_TRANSIENT_MORTAL_CACHE_VALUE_RESPONSE)
public class SuccessfulMetadataTransientMortalCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulMetadataTransientMortalCacheValueResponse(MetadataTransientMortalCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public MetadataTransientMortalCacheValue getResponseValue() {
      return (MetadataTransientMortalCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulMetadataTransientMortalCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
