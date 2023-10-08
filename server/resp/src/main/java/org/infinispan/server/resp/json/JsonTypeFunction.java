package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

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
import com.jayway.jsonpath.JsonPath;

@ProtoTypeId(ProtoStreamTypeIds.RESP_JSON_TYPE_FUNCTION)
public class JsonTypeFunction implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<String>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   private static final String STRING = "string";
   private static final String BOOLEAN = "boolean";
   private static final String INTEGER = "integer";
   private static final String NUMBER = "number";
   private static final String ARRAY = "array";
   private static final String NULL = "null";
   private static final String UNKNOWN = "unknown";
   private static final String OBJECT = "object";

   @ProtoField(1)
   final byte[] path;

   @ProtoFactory
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
}
