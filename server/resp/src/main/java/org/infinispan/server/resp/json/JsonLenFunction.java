package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commons.CacheException;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

abstract class JsonLenFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   @ProtoField(1)
   protected final byte[] path;
   protected final Predicate<JsonNode> condition;
   protected final Function<JsonNode, Long> mapper;

   protected JsonLenFunction(byte[] path,  Predicate<JsonNode> condition, Function<JsonNode, Long> mapper) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
      this.condition = condition;
      this.mapper = mapper;
   }

   @Override
   public List<Long> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      var pathStr = new String(path, StandardCharsets.UTF_8);
      if (doc == null) {
         return null;
      }
      try {
         var rootNode = JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode nodeList = jpCtx.read(jpath);
         List<Long> result = new ArrayList<>();
         addNodeSize(result, nodeList, condition, mapper);
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
