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

public class JsonNumMultOpFunction extends JsonNumOpFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Number>> {
   public static final AdvancedExternalizer<JsonNumMultOpFunction> EXTERNALIZER = new JsonNumMultOpFunction.Externalizer();

   public JsonNumMultOpFunction(byte[] path, byte[] increment) {
      super(path, increment);
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

   private static class Externalizer implements AdvancedExternalizer<JsonNumMultOpFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonNumMultOpFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
         JSONUtil.writeBytes(output, object.value);
      }

      @Override
      public JsonNumMultOpFunction readObject(ObjectInput input) throws IOException {
         byte[] path = JSONUtil.readBytes(input);
         byte[] increment = JSONUtil.readBytes(input);
         return new JsonNumMultOpFunction(path, increment);
      }

      @Override
      public Set<Class<? extends JsonNumMultOpFunction>> getTypeClasses() {
         return Collections.singleton(JsonNumMultOpFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_NUM_MULT_BY_FUNCTION;
      }
   }

}
