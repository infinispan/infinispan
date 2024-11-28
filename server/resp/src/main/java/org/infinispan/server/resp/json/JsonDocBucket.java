package org.infinispan.server.resp.json;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store Set data type.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_JSONDOC_BUCKET)
public class JsonDocBucket {
   final String value;

   @ProtoFactory
   public JsonDocBucket(String value) {
      this.value = value;
   }

   @ProtoField(number = 1)
   public String getValue() {
      return this.value;
   }

   @Override
   public String toString() {
      return "JsonDocBucket{value=" + value + "}";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      JsonDocBucket jsonBucket = (JsonDocBucket) o;
      return Objects.equals(this.value, jsonBucket.value);
   }

   @Override
   public int hashCode() {
      return Objects.hash(value);
   }
}
