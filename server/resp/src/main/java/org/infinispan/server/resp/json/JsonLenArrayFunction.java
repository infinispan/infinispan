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

public class JsonLenArrayFunction extends JsonLenFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final AdvancedExternalizer<JsonLenArrayFunction> EXTERNALIZER = new JsonLenArrayFunction.Externalizer();

   public JsonLenArrayFunction(byte[] path) {
     super(path, JsonNode::isArray, jsonNode -> (long) jsonNode.size());
   }

   private static class Externalizer implements AdvancedExternalizer<JsonLenArrayFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonLenArrayFunction jsonLenFunction) throws IOException {
         JSONUtil.writeBytes(output, jsonLenFunction.path);
      }

      @Override
      public JsonLenArrayFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonLenArrayFunction(path);
      }

      @Override
      public Set<Class<? extends JsonLenArrayFunction>> getTypeClasses() {
         return Collections.singleton(JsonLenArrayFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_LEN_ARR_FUNCTION;
      }
   }

}
