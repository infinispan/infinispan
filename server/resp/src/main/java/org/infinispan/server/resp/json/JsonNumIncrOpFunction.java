package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class JsonNumIncrOpFunction extends JsonNumOpFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Number>> {
   public static final AdvancedExternalizer<JsonNumIncrOpFunction> EXTERNALIZER = new JsonNumIncrOpFunction.Externalizer();

   public JsonNumIncrOpFunction(byte[] path, byte[] increment) {
      super(path, increment);
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

   private static class Externalizer implements AdvancedExternalizer<JsonNumIncrOpFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonNumIncrOpFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
         JSONUtil.writeBytes(output, object.value);
      }

      @Override
      public JsonNumIncrOpFunction readObject(ObjectInput input) throws IOException {
         byte[] path = JSONUtil.readBytes(input);
         byte[] increment = JSONUtil.readBytes(input);
         return new JsonNumIncrOpFunction(path, increment);
      }

      @Override
      public Set<Class<? extends JsonNumIncrOpFunction>> getTypeClasses() {
         return Collections.singleton(JsonNumIncrOpFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_NUM_INCR_BY_FUNCTION;
      }
   }

}
