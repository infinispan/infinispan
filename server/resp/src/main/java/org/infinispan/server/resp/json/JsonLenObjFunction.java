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

public class JsonLenObjFunction extends JsonLenFunction
      implements SerializableFunction<ReadWriteEntryView<byte[], JsonBucket>, List<Long>> {
   public static final AdvancedExternalizer<JsonLenObjFunction> EXTERNALIZER = new JsonLenObjFunction.Externalizer();

   public JsonLenObjFunction(byte[] path) {
      super(path, JsonNode::isObject, jsonNode -> (long) jsonNode.size());
   }

   private static class Externalizer implements AdvancedExternalizer<JsonLenObjFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonLenObjFunction jsonLenFunction) throws IOException {
         JSONUtil.writeBytes(output, jsonLenFunction.path);
      }

      @Override
      public JsonLenObjFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] path = JSONUtil.readBytes(input);
         return new JsonLenObjFunction(path);
      }

      @Override
      public Set<Class<? extends JsonLenObjFunction>> getTypeClasses() {
         return Collections.singleton(JsonLenObjFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_LEN_OBJ_FUNCTION;
      }
   }

}
