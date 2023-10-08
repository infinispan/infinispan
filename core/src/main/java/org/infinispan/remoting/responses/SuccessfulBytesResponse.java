package org.infinispan.remoting.responses;

import java.util.Arrays;
import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_BYTES_RESPONSE)
public class SuccessfulBytesResponse implements SuccessfulResponse<byte[]> {

   @ProtoField(1)
   final byte[] bytes;

   @ProtoFactory
   static SuccessfulBytesResponse protoFactory(byte[] bytes) {
      return bytes == null ? null : new SuccessfulBytesResponse(bytes);
   }

   SuccessfulBytesResponse(byte[] bytes) {
      this.bytes = bytes;
   }

   public byte[] getResponseValue() {
      return bytes;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulBytesResponse that = (SuccessfulBytesResponse) o;
      return Objects.deepEquals(bytes, that.bytes);
   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(bytes);
   }

   @Override
   public String toString() {
      return "SuccessfulByteResponse{" +
            "bytes=" + Arrays.toString(bytes) +
            '}';
   }
}
