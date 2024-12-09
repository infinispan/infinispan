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
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";

   byte[] value;
   byte[] path;
   boolean nx;
   boolean xx;

   static Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   public JsonDocSetFunction(byte[] value, byte[] path, boolean nx, boolean xx) {
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
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
         if (!isRoot()) {
            throw new CacheException("new objects must be created at root");
         }
         entryView.set(new JsonDocBucket(value));
         return RespConstants.OK;
      }
      if (nx) {
         return null;
      }
      if (isRoot()) {
         // Updating the root node is not allowed by jsonpath
         // replacing the whole doc here
         entryView.set(new JsonDocBucket(value));
         return RespConstants.OK;
      }
      try {
         var rootObjectNode = (ObjectNode) JSONUtil.objectMapper.readTree(doc.getValue());
         var jpCtx = JsonPath.using(config).parse(rootObjectNode);
         var pathStr = new String(path, StandardCharsets.UTF_8);
         JsonNode node = jpCtx.read(pathStr);
         if (node.isNull() && xx || !node.isNull() && nx) {
            return null;
         }
         jpCtx.set(pathStr, newNode);
         entryView.set(new JsonDocBucket(JSONUtil.objectMapper.writeValueAsBytes(rootObjectNode)));
         return RespConstants.OK;
      } catch (PathNotFoundException ex) {
         // mimicking redis. Not an error, do nothing and return null
         return null;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private boolean isRoot() {
      return path != null && path.length == 1 && path[0] == '$';
   }

   private static class Externalizer implements AdvancedExternalizer<JsonDocSetFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonDocSetFunction object) throws IOException {
         output.writeObject(object.value);
         output.writeObject(object.path);
         output.writeBoolean(object.nx);
         output.writeBoolean(object.xx);
      }

      @Override
      public JsonDocSetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] value = (byte[]) input.readObject();
         byte[] path = (byte[]) input.readObject();
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
