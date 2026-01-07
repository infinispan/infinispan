package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.CacheException;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

abstract class JsonNumOpFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Number>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final String ERR_INCREMENT_CANT_BE_NULL = "increment can't be null";

   @ProtoField(1)
   protected final byte[] path;

   @ProtoField(2)
   protected final byte[] value;

   protected JsonNumOpFunction(byte[] path, byte[] value) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      requireNonNull(value, ERR_INCREMENT_CANT_BE_NULL);
      this.value = value;
      this.path = path;
   }

   @Override
   public List<Number> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      JsonNode incrNode;
      try {
         incrNode = JSONUtil.objectMapper.readTree(value);
         if (!incrNode.isNumber()) {
            throw new IllegalArgumentException("Not a valid increment number: " + incrNode.asText());
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
         List<String> resPathList = new ArrayList<>(pathList.size());
         for (JsonNode pathAsNode : pathList) {
            String pathAsText = pathAsNode.asText();
            ArrayNode node = getForContext.read(pathAsText);
            if (node.get(0).isNumber()) {
               if (JSONUtil.isRoot(pathAsText.getBytes(StandardCharsets.UTF_8))) {
                  // We are changing the root !
                  Number incremented = operate(node.get(0), incrNode);
                  if (!isFinite(incremented)) {
                     // Do nothing and return null, will be handled by the caller
                     return null;
                  }
                  String jsonNumberValue = "" + incremented + "";
                  entryView.set(new JsonBucket(jsonNumberValue.getBytes(StandardCharsets.UTF_8)));
                  resList.add(incremented);
                  // changed root, returning
                  return resList;
               }
               Number incremented = operate(node.get(0), incrNode);
               if (!isFinite(incremented)) {
                  // Do nothing and return null, will be handled by the caller
                  return null;
               }
               resPathList.add(pathAsText);
               resList.add(incremented);
            } else {
               resPathList.add(pathAsText);
               resList.add(null);
            }
         }
         // Apply all modifications after processing all paths
         for (int i = 0; i < resList.size(); i++) {
            Number n = resList.get(i);
            if (n != null) {
               modifiableCtx.set(resPathList.get(i), resList.get(i));
               changed = true;
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

   private boolean isFinite(Number n) {
      if (n instanceof Double || n instanceof Float) {
         return Double.isFinite(n.doubleValue());
      }
      return true;
   }

   protected abstract Number operate(JsonNode numNode, JsonNode incrNode);
}
