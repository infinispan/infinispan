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
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

public class JsonDebugMemoryFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final AdvancedExternalizer<JsonDebugMemoryFunction> EXTERNALIZER = new JsonDebugMemoryFunction.Externalizer();

   byte[] path;

   public JsonDebugMemoryFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public List<Integer> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      if (doc == null) {
        return new ArrayList<>(0);
      }

      var pathStr = new String(path, StandardCharsets.UTF_8);
      try {
         var rootNode = JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode nodeList = jpCtx.read(jpath);
         List<Integer> resList = new ArrayList<>(nodeList.size());
         for (JsonNode jsonNode : nodeList) {
            resList.add((2 * jsonNode.toString().getBytes().length) + 38);
         }
         return resList;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonDebugMemoryFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonDebugMemoryFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
      }

      @Override
      public JsonDebugMemoryFunction readObject(ObjectInput input) throws IOException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonDebugMemoryFunction(path);
      }

      @Override
      public Set<Class<? extends JsonDebugMemoryFunction>> getTypeClasses() {
         return Collections.singleton(JsonDebugMemoryFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_DEBUG_FUNCTION;
      }
   }

}
