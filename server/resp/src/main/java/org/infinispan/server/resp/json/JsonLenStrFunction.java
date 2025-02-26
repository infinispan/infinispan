package org.infinispan.server.resp.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class JsonLenStrFunction extends JsonLenFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final AdvancedExternalizer<JsonLenStrFunction> EXTERNALIZER = new JsonLenStrFunction.Externalizer();

   public JsonLenStrFunction(byte[] path) {
      super(path, JsonNode::isTextual, jsonNode -> (long) jsonNode.asText().length());
   }

   private static class Externalizer implements AdvancedExternalizer<JsonLenStrFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonLenStrFunction jsonLenFunction) throws IOException {
         JSONUtil.writeBytes(output, jsonLenFunction.path);
      }

      @Override
      public JsonLenStrFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonLenStrFunction(path);
      }

      @Override
      public Set<Class<? extends JsonLenStrFunction>> getTypeClasses() {
         return Collections.singleton(JsonLenStrFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_LEN_STR_FUNCTION;
      }
   }

}
