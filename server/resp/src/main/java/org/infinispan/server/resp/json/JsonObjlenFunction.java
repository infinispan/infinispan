package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;

public class JsonObjlenFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final AdvancedExternalizer<JsonObjlenFunction> EXTERNALIZER = new JsonObjlenFunction.Externalizer();

   byte[] path;

   public JsonObjlenFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public List<Long> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = (JsonBucket) entryView.find().orElse(null);
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
            for (JsonNode jsonNode : nodeList) {
               if (jsonNode.isObject()) {
                  result.add((long)((ObjectNode)jsonNode).size());
               } else {
                  result.add(null);
               }
            }
         return result;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonObjlenFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonObjlenFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
      }

      @Override
      public JsonObjlenFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonObjlenFunction(path);
      }

      @Override
      public Set<Class<? extends JsonObjlenFunction>> getTypeClasses() {
         return Collections.singleton(JsonObjlenFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_OBJLEN_FUNCTION;
      }
   }

}
