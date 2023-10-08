package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_METADATA_MORTAL_CACHE_VALUE_RESPONSE)
public class SuccessfulMetadataMortalCacheValueResponse extends AbstractInternalCacheValueResponse {

   @ProtoFactory
   SuccessfulMetadataMortalCacheValueResponse(MetadataMortalCacheValue responseValue) {
      super(responseValue);
   }

   @ProtoField(1)
   public MetadataMortalCacheValue getResponseValue() {
      return (MetadataMortalCacheValue) icv;
   }

   @Override
   public String toString() {
      return "SuccessfulMetadataMortalCacheValueResponse{" +
            "icv=" + icv +
            '}';
   }
}
