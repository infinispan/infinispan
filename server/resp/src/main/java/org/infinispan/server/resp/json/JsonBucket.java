package org.infinispan.server.resp.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.server.resp.ExternalizerIds;

/**
 * Bucket used to store Set data type.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
public record JsonBucket(byte[] value) {

   public static final AdvancedExternalizer<JsonBucket> EXTERNALIZER = new JsonBucket.Externalizer();

   public byte[] value() {
      return value;
   }

   private static class Externalizer implements AdvancedExternalizer<JsonBucket> {

      @Override
      public void writeObject(ObjectOutput output, JsonBucket object) throws IOException {
         JSONUtil.writeBytes(output, object.value);
      }

      @Override
      public JsonBucket readObject(ObjectInput input) throws IOException {
         return new JsonBucket(JSONUtil.readBytes(input));
      }

      @Override
      public Set<Class<? extends JsonBucket>> getTypeClasses() {
         return Collections.singleton(JsonBucket.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_BUCKET;
      }
   }
}
