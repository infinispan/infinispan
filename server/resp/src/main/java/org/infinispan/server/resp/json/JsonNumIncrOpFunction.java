package org.infinispan.server.resp.json;

import java.math.BigDecimal;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

import com.fasterxml.jackson.databind.JsonNode;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_NUM_INCR_BY_FUNCTION)
public class JsonNumIncrOpFunction extends JsonNumOpFunction {

   @ProtoFactory
   public JsonNumIncrOpFunction(byte[] path, byte[] value) {
      super(path, value);
   }

   @Override
   protected Number operate(JsonNode numNode, JsonNode incrNode) {
      if (numNode.isDouble() || incrNode.isDouble()) {
         return numNode.doubleValue() + incrNode.doubleValue();
      }

      if (numNode.isFloat() || incrNode.isFloat()) {
         return numNode.floatValue() + incrNode.floatValue();
      }

      if (numNode.isLong() || incrNode.isLong()) {
         return numNode.longValue() + incrNode.longValue();
      }

      if (numNode.isBigInteger() || incrNode.isBigInteger()) {
         return numNode.bigIntegerValue().add(incrNode.bigIntegerValue());
      }

      if (numNode.isBigDecimal() || incrNode.isBigDecimal()) {
         return new BigDecimal(numNode.asText()).add(new BigDecimal(incrNode.asText()));
      }

      return numNode.intValue() + incrNode.intValue();
   }
}
