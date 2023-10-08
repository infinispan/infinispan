package org.infinispan.remoting.responses;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.SUCCESSFUL_BOOLEAN_RESPONSE)
public class SuccessfulBooleanResponse implements SuccessfulResponse<Boolean> {

   final Boolean value;

   @ProtoFactory
   SuccessfulBooleanResponse(Boolean responseValue) {
      this.value = responseValue;
   }

   @Override
   @ProtoField(1)
   public Boolean getResponseValue() {
      return value;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      SuccessfulBooleanResponse that = (SuccessfulBooleanResponse) o;
      return Objects.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(value);
   }

   @Override
   public String toString() {
      return "SuccessfulBooleanResponse{" +
            "value=" + value +
            '}';
   }
}
