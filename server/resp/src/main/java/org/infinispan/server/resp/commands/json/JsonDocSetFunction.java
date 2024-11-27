package org.infinispan.server.resp.commands.json;

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
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

public class JsonDocSetFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonDocBucket>, String> {
   String value;
   String path;
   boolean nx;
   boolean xx;

   static Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   public JsonDocSetFunction(String value, String path, boolean nx, boolean xx) {
      this.value = value;
      this.path = path;
      this.nx = nx;
      this.xx = xx;
   }

   @Override
   public String apply(ReadWriteEntryView<byte[], JsonDocBucket> entryView) {
      JsonNode newNode;
      try {
         newNode = JSONUtil.objectMapper.readTree(value);
      } catch (IOException e) {
         throw new CacheException(e);
      }
      var doc = (JsonDocBucket) entryView.find().orElse(null);
      if (doc == null) {
         if (xx) {
            return null;
         }
         if (!"$".equals(path)) {
            throw new CacheException("new objects must be created at root");
         }
         entryView.set(new JsonDocBucket(value.getBytes(StandardCharsets.UTF_8)));
         return RespConstants.OK;
      }
      if (nx) {
         return null;
      }
      if ("$".equals(path)) {
         // Updating the root node is not allowed by jsonpath
         // replacing the whole doc here
         entryView.set(new JsonDocBucket(value.getBytes(StandardCharsets.UTF_8)));
         return RespConstants.OK;
      }
      try {
         var rootObjectNode = (ObjectNode) JSONUtil.objectMapper.readTree(doc.getValue());
         var jpCtx = JsonPath.using(config).parse(rootObjectNode);
         JsonNode node = jpCtx.read(path);
         if (node.isNull() && xx || !node.isNull() && nx) {
            return null;
         }
         jpCtx.set(path, newNode);
         entryView.set(new JsonDocBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
         return RespConstants.OK;
      } catch (PathNotFoundException ex) {
         // mimicking redis. Not an error, do nothing and return null
         return null;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonDocSetFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonDocSetFunction object) throws IOException {
         output.writeUTF(object.value);
         output.writeUTF(object.path);
         output.writeBoolean(object.nx);
         output.writeBoolean(object.xx);
      }

      @Override
      public JsonDocSetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String value = input.readUTF();
         String path = input.readUTF();
         boolean nx = input.readBoolean();
         boolean xx = input.readBoolean();
         return new JsonDocSetFunction(value, path, nx, xx);
      }

      @Override
      public Set<Class<? extends JsonDocSetFunction>> getTypeClasses() {
         return Collections.singleton(JsonDocSetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSONDOC_SET_FUNCTION;
      }
   }

}
