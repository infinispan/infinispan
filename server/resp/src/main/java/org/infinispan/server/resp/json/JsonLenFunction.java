package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_LEN_FUNCTION)
public class JsonLenFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   @ProtoField(1)
   final byte[] path;

   @ProtoField(2)
   final LenType lenType;

   @ProtoFactory
   public JsonLenFunction(byte[] path, LenType lenType) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.lenType = lenType;
      this.path = path;
   }

   @Override
   public List<Long> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      var pathStr = new String(path, StandardCharsets.UTF_8);
      if (doc == null) {
         return null;
      }
      try {
         var rootNode = (ObjectNode) JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode nodeList = jpCtx.read(jpath);
         List<Long> result = new ArrayList<>();
         switch (lenType) {
            case OBJECT -> addNodeSize(result, nodeList, JsonNode::isObject, jsonNode -> (long) jsonNode.size());
            case STRING -> addNodeSize(result, nodeList, JsonNode::isTextual, jsonNode -> (long) jsonNode.asText().length());
            case ARRAY -> addNodeSize(result, nodeList, JsonNode::isArray, jsonNode -> (long) jsonNode.size());
         }
         return result;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private void addNodeSize(List<Long> result, ArrayNode nodeList, Predicate<JsonNode> condition, Function<JsonNode, Long> mapper) {
      nodeList.forEach(jsonNode -> result.add(condition.test(jsonNode) ? mapper.apply(jsonNode) : null));
   }
}
