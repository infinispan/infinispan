package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
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

import static java.util.Objects.requireNonNull;

public class JsonTypeFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<String>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final AdvancedExternalizer<JsonTypeFunction> EXTERNALIZER = new JsonTypeFunction.Externalizer();
   private static final String STRING = "string";
   private static final String BOOLEAN = "boolean";
   private static final String INTEGER = "integer";
   private static final String NUMBER = "number";
   private static final String ARRAY = "array";
   private static final String NULL = "null";
   private static final String UNKNOWN = "unknown";
   private static final String OBJECT = "object";

   byte[] path;

   public JsonTypeFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public List<String> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc =  entryView.find().orElse(null);
      var pathStr = new String(path, StandardCharsets.UTF_8);
      if (doc == null) {
         return null;
      }
      try {
         var rootNode = JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode nodeList = jpCtx.read(jpath);
         List<String> result = new ArrayList<>();
         for (JsonNode jsonNode : nodeList) {
            result.add(mapType(jsonNode));
         }
         return result;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   /**
    * Maps a {@link JsonNode} to its corresponding JSON type as a string.
    * <p>
    * The method returns one of the following type strings based on the value of the {@code jsonNode}:
    * </p>
    * <ul>
    *   <li>{@code "string"} - If the value is a text/string.</li>
    *   <li>{@code "boolean"} - If the value is a boolean (true or false).</li>
    *   <li>{@code "integer"} - If the value is an integer.</li>
    *   <li>{@code "number"} - If the value is a number (including floating-point values).</li>
    *   <li>{@code "array"} - If the value is a JSON array.</li>
    *   <li>{@code "null"} - If the value is null.</li>
    *   <li>{@code "object"} - If the value is a JSON object.</li>
    *   <li>{@code "unknown"} - If the type cannot be determined.</li>
    * </ul>
    *
    * @param jsonNode The {@link JsonNode} to be mapped to a string type.
    * @return A string representing the JSON type of the given {@code jsonNode}.
    */
   private String mapType(JsonNode jsonNode) {
      if (jsonNode.isTextual()) {
         return STRING;
      }
      if (jsonNode.isBoolean()) {
         return BOOLEAN;
      }
      if (jsonNode.isInt()) {
         return INTEGER;
      }
      if (jsonNode.isNumber()) {
         return NUMBER;
      }
      if (jsonNode.isArray()) {
         return ARRAY;
      }
      if (jsonNode.isNull()) {
         return NULL;
      }
      if (jsonNode.isObject()) {
         return OBJECT;
      }
      return UNKNOWN;
   }

   private static class Externalizer implements AdvancedExternalizer<JsonTypeFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonTypeFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
      }

      @Override
      public JsonTypeFunction readObject(ObjectInput input) throws IOException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonTypeFunction(path);
      }

      @Override
      public Set<Class<? extends JsonTypeFunction>> getTypeClasses() {
         return Collections.singleton(JsonTypeFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_TYPE_FUNCTION;
      }
   }

}
