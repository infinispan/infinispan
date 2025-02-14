package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
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

public class JsonNumIncrByFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Number>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final String ERR_INCREMENT_CANT_BE_NULL = "increment can't be null";
   public static final AdvancedExternalizer<JsonNumIncrByFunction> EXTERNALIZER = new JsonNumIncrByFunction.Externalizer();

   byte[] path;
   byte[] increment;

   public JsonNumIncrByFunction(byte[] path, byte[] increment) {
       requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
       requireNonNull(increment, ERR_INCREMENT_CANT_BE_NULL);
       this.increment = increment;
       this.path = path;
   }

   @Override
   public List<Number> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      JsonNode incrNode;
      try {
         incrNode = JSONUtil.objectMapper.readTree(increment);
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
               Number incremented = incrementValue(node.get(0), incrNode);
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

   private Number incrementValue(JsonNode numNode, JsonNode incrNode) {
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

   private static class Externalizer implements AdvancedExternalizer<JsonNumIncrByFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonNumIncrByFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
         JSONUtil.writeBytes(output, object.increment);
      }

      @Override
      public JsonNumIncrByFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] path = JSONUtil.readBytes(input);
         byte[] increment = JSONUtil.readBytes(input);
         return new JsonNumIncrByFunction(path, increment);
      }

      @Override
      public Set<Class<? extends JsonNumIncrByFunction>> getTypeClasses() {
         return Collections.singleton(JsonNumIncrByFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_NUMINCRBY_FUNCTION;
      }
   }

}
