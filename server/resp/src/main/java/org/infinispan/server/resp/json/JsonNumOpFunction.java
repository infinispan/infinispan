package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class JsonNumOpFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Number>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final String ERR_INCREMENT_CANT_BE_NULL = "increment can't be null";
   public static final AdvancedExternalizer<JsonNumOpFunction> EXTERNALIZER = new JsonNumOpFunction.Externalizer();

   byte[] path;
   byte[] value;
   NumOpType numOpType;

   public JsonNumOpFunction(byte[] path, byte[] increment, NumOpType numOpType) {
       requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
       requireNonNull(increment, ERR_INCREMENT_CANT_BE_NULL);
       this.value = increment;
       this.path = path;
       this.numOpType = numOpType;
   }

   @Override
   public List<Number> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      JsonNode incrNode;
      try {
         incrNode = JSONUtil.objectMapper.readTree(value);
         if (!incrNode.isNumber()) {
            throw new IllegalArgumentException("Non a valid increment number: " + incrNode.asText());
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }

      var doc = entryView.find().orElse(null);
      if (doc == null) {
         throw new CacheException("could not perform this operation on a key that doesn't exist");
      }

      var pathStr = new String(path, StandardCharsets.UTF_8);
      try {
         JsonNode rootNode = JSONUtil.objectMapper.readTree(doc.value());
         DocumentContext modifiableCtx = JSONUtil.parserForMod.parse(rootNode);
         DocumentContext getForContext = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode pathList = modifiableCtx.read(jpath);
         boolean changed = false;
         List<Number> resList = new ArrayList<>(pathList.size());
         for (JsonNode pathAsNode : pathList) {
            String pathAsText = pathAsNode.asText();
            ArrayNode node = getForContext.read(pathAsText);
            if (node.get(0).isNumber()) {
               Number incremented = operate(node.get(0), incrNode);
               modifiableCtx.set(pathAsText, incremented);
               resList.add(incremented);
               changed = true;
            } else {
               resList.add(null);
            }
         }
         if (changed) {
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
         }
         return resList;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private Number operate(JsonNode numNode, JsonNode valueNode) {
      switch (numOpType){
         case INCR -> {
            return increment(numNode, valueNode);
         }
         case MULT -> {
            return multiply(numNode, valueNode);
         }
      }
      throw new IllegalArgumentException("Unknown numOpType: " + numOpType);
   }

   private Number increment(JsonNode numNode, JsonNode incrNode) {
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

   private Number multiply(JsonNode numNode, JsonNode multiply) {
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

   private static class Externalizer implements AdvancedExternalizer<JsonNumOpFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonNumOpFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
         JSONUtil.writeBytes(output, object.value);
         MarshallUtil.marshallEnum(object.numOpType, output);
      }

      @Override
      public JsonNumOpFunction readObject(ObjectInput input) throws IOException {
         byte[] path = JSONUtil.readBytes(input);
         byte[] increment = JSONUtil.readBytes(input);
         var numOpType = MarshallUtil.unmarshallEnum(input, NumOpType::valueOf);
         return new JsonNumOpFunction(path, increment, numOpType);
      }

      @Override
      public Set<Class<? extends JsonNumOpFunction>> getTypeClasses() {
         return Collections.singleton(JsonNumOpFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_NUMOPBY_FUNCTION;
      }
   }

}
