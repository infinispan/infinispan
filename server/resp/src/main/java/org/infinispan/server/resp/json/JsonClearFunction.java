package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_CLEAR_FUNCTION)
public class JsonClearFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, Integer> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   @ProtoField(1)
   final byte[] path;

   @ProtoFactory
   public JsonClearFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public Integer apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
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
         int changed = 0;
         for (JsonNode pathAsNode : pathList) {
            String pathAsText = pathAsNode.asText();
            ArrayNode node = getForContext.read(pathAsText);
            JsonNode jsonNode = node.get(0);
            if (JSONUtil.isRoot(pathAsText.getBytes(StandardCharsets.UTF_8))) {
               // if the node is root, we need to replace all the value.
               TreeNode clearedNode = null;
               if (jsonNode.isObject()) {
                  clearedNode = JSONUtil.objectMapper.createObjectNode();
               } else if (jsonNode.isArray()) {
                  clearedNode = JSONUtil.objectMapper.createArrayNode();
               } else if (jsonNode.isNumber()){
                  clearedNode = JSONUtil.objectMapper.valueToTree(0);
               }

               if (clearedNode == null) {
                  // Do nothing
                  return 0;
               }

               entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(clearedNode)));
               return 1;
            }

            if (jsonNode.isObject()) {
               ObjectNode objectNode = (ObjectNode) node.get(0);
               if (!objectNode.properties().isEmpty()) {
                  modifiableCtx.set(pathAsText, JSONUtil.objectMapper.createObjectNode());
                  changed++;
               }
            } else if (jsonNode.isArray()) {
               ArrayNode arrayNode = (ArrayNode) node.get(0);
               if (!arrayNode.isEmpty()) {
                  modifiableCtx.set(pathAsText, JSONUtil.objectMapper.createArrayNode());
                  changed++;
               }
            } else if (jsonNode.isNumber()) {
               Number numberValue = node.get(0).numberValue();
               if (numberValue.intValue() != 0) {
                  modifiableCtx.set(pathAsText, 0);
                  changed++;
               }
            }
         }
         if (changed > 0) {
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
         }
         return changed;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
