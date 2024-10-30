package org.infinispan.server.resp.json;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store Set data type.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_BUCKET)
public record JsonBucket(byte[] value) {

   @ProtoField(number = 1)
   public byte[] value() {
      return value;
   }

}
