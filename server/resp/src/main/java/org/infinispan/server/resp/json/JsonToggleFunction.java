package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.jayway.jsonpath.DocumentContext;
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

public class JsonToggleFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Integer>> {
   public static final String ERR_PATH_CAN_T_BE_NULL = "path can't be null";
   public static final AdvancedExternalizer<JsonToggleFunction> EXTERNALIZER = new JsonToggleFunction.Externalizer();

   byte[] path;

   public JsonToggleFunction(byte[] path) {
      requireNonNull(path, ERR_PATH_CAN_T_BE_NULL);
      this.path = path;
   }

   @Override
   public List<Integer> apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      var doc = entryView.find().orElse(null);
      if (doc == null) {
         throw new CacheException("could not perform this operation on a key that doesn't exist");
      }

      var pathStr = new String(path, StandardCharsets.UTF_8);
      try {
         JsonNode rootNode = JSONUtil.objectMapper.readTree(doc.value());
         DocumentContext modifiableCtx = JSONUtil.parserForMod.parse(rootNode);
         DocumentContext getForContext = JSONUtil.parserForGet.parse(rootNode);
         JsonPath jpath = JsonPath.compile(pathStr);
         ArrayNode pathList = modifiableCtx.read(jpath);
         boolean changed = false;
         List<Integer> resList = new ArrayList<>(pathList.size());
         for (JsonNode pathAsNode : pathList) {
            String pathAsText = pathAsNode.asText();
            ArrayNode node = getForContext.read(pathAsText);
            if (node.get(0).isBoolean()) {
               boolean currentBolValue = node.get(0).asBoolean();
               modifiableCtx.set(pathAsText, currentBolValue? BooleanNode.FALSE : BooleanNode.TRUE);
               resList.add(currentBolValue? 0 : 1);
               changed = true;
            } else {
               resList.add(null);
            }
         }
         if (changed) {
            entryView.set(new JsonBucket(JSONUtil.objectMapper.writeValueAsBytes(rootNode)));
         }
         return resList;
      } catch (CacheException e) {
         throw e;
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonToggleFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonToggleFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.path);
      }

      @Override
      public JsonToggleFunction readObject(ObjectInput input) throws IOException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonToggleFunction(path);
      }

      @Override
      public Set<Class<? extends JsonToggleFunction>> getTypeClasses() {
         return Collections.singleton(JsonToggleFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_TOGGLE_FUNCTION;
      }
   }

}
