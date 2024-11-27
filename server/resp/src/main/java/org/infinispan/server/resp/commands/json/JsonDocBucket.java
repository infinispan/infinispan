package org.infinispan.server.resp.commands.json;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
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
   final byte[] value;

   @ProtoFactory
   public JsonDocBucket(byte[] value) {
      this.value = value;
   }

   @ProtoField(number = 1)
   public byte[] getValue() {
      return this.value;
   }

   @Override
   public String toString() {
      return "SetBucket{values=" + Util.toStr(value) + '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JsonDocBucket jsonBucket = (JsonDocBucket) o;
      if (this.value.length != jsonBucket.value.length) return false;
      int pos = 0;
      while (true) {
         if (pos == this.value.length) return true;
         if (this.value[pos] != jsonBucket.value[pos++]) return false;
      }
   }

   @Override
   public int hashCode() {
      return Objects.hash(value);
   }
}
