package org.infinispan.remoting.responses;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_LONG_RESPONSE)
public class SuccessfulLongResponse implements SuccessfulResponse<Long> {

   final Long value;

   @ProtoFactory
   SuccessfulLongResponse(Long responseValue) {
      this.value = responseValue;
   }

   @Override
   @ProtoField(1)
   public Long getResponseValue() {
      return value;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulLongResponse that = (SuccessfulLongResponse) o;
      return Objects.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(value);
   }

   @Override
   public String toString() {
      return "SuccessfulLongResponse{" +
            "value=" + value +
            '}';
   }
}
