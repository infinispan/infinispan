package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class JsonLenFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final AdvancedExternalizer<JsonLenFunction> EXTERNALIZER = new JsonLenFunction.Externalizer();
   private final LenType lenType;

   byte[] path;

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

   private static class Externalizer implements AdvancedExternalizer<JsonLenFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonLenFunction jsonLenFunction) throws IOException {
         MarshallUtil.marshallEnum(jsonLenFunction.lenType, output);
         JSONUtil.writeBytes(output, jsonLenFunction.path);
      }

      @Override
      public JsonLenFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         var lenType = MarshallUtil.unmarshallEnum(input, LenType::valueOf);
         byte[] path = JSONUtil.readBytes(input);
         return new JsonLenFunction(path, lenType);
      }

      @Override
      public Set<Class<? extends JsonLenFunction>> getTypeClasses() {
         return Collections.singleton(JsonLenFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_LEN_FUNCTION;
      }
   }

}
