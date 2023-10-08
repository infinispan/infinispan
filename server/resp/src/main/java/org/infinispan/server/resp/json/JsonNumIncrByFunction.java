package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_NUM_INCR_BY_FUNCTION)
public class JsonNumIncrByFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Number>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final String ERR_INCREMENT_CANT_BE_NULL = "increment can't be null";

   @ProtoField(1)
   final byte[] path;

   @ProtoField(2)
   final byte[] increment;

   @ProtoFactory
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
}
