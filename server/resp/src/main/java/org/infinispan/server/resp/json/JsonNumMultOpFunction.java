package org.infinispan.server.resp.json;

import java.math.BigDecimal;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

import com.fasterxml.jackson.databind.JsonNode;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_NUM_MULT_FUNCTION)
public class JsonNumMultOpFunction extends JsonNumOpFunction {

   @ProtoFactory
   public JsonNumMultOpFunction(byte[] path, byte[] value) {
      super(path, value);
   }

   @Override
   protected Number operate(JsonNode numNode, JsonNode multiply) {
      if (numNode.isDouble() || multiply.isDouble()) {
         return numNode.doubleValue() * multiply.doubleValue();
      }

      if (numNode.isFloat() || multiply.isFloat()) {
         return numNode.floatValue() * multiply.floatValue();
      }

      if (numNode.isLong() || multiply.isLong()) {
         return numNode.longValue() * multiply.longValue();
      }

      if (numNode.isBigInteger() || multiply.isBigInteger()) {
         return numNode.bigIntegerValue().multiply(multiply.bigIntegerValue());
      }

      if (numNode.isBigDecimal() || multiply.isBigDecimal()) {
         return new BigDecimal(numNode.asText()).multiply(new BigDecimal(multiply.asText()));
      }

      return numNode.intValue() * multiply.intValue();
   }
}
