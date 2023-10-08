package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_METADATA_TRANSIENT_CACHE_VALUE_RESPONSE)
public class SuccessfulMetadataTransientCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulMetadataTransientCacheValueResponse(MetadataTransientCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public MetadataTransientCacheValue getResponseValue() {
      return (MetadataTransientCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulMetadataTransientCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
