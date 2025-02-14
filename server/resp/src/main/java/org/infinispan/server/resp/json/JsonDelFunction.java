package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.JsonPath;

public class JsonDelFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, Long> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final AdvancedExternalizer<JsonDelFunction> EXTERNALIZER = new JsonDelFunction.Externalizer();

   byte[] path;

   public JsonDelFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public Long apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      if (doc == null) {
         return 0L;
      }
      byte[] jsonPath = JSONUtil.toJsonPath(path);
      try {
         JsonNode rootObjectNode = JSONUtil.objectMapper.readTree(doc.value());
         var jpCtx = JSONUtil.parserForGet.parse(rootObjectNode);
         if (JSONUtil.isRoot(jsonPath)) {
            entryView.remove();
            return 1L;
         }
         var pathStr = new String(jsonPath, StandardCharsets.UTF_8);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode an = jpCtx.read(jpath);
         if (an.size() == 0 || an.size() == 1 && an.get(0) == null) {
            return 0L;
         }
         jpCtx = jpCtx.delete(jpath);
         entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
         return (long) an.size();
      } catch (IndexOutOfBoundsException e) {
         // Trying to delete a non existent index of an array return 0
         return 0L;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonDelFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonDelFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
      }

      @Override
      public JsonDelFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonDelFunction(path);
      }

      @Override
      public Set<Class<? extends JsonDelFunction>> getTypeClasses() {
         return Collections.singleton(JsonDelFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_DEL_FUNCTION;
      }
   }

}
