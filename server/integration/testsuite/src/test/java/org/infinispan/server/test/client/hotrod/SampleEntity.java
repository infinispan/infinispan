package org.infinispan.server.test.client.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

/**
 * @author gustavonalle
 * @since 8.0
 */
@SerializeWith(SampleEntity.SampleEntityExternalizer.class)
public class SampleEntity {

   private String csvAttributes;

   public SampleEntity(String csvAttributes) {
      this.csvAttributes = csvAttributes;
   }

   public String getCsvAttributes() {
      return csvAttributes;
   }

   public static class SampleEntityExternalizer implements Externalizer<SampleEntity> {

      @Override
      public void writeObject(ObjectOutput output, SampleEntity object) throws IOException {
         output.writeUTF(object.getCsvAttributes());
      }

      @Override
      public SampleEntity readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SampleEntity(input.readUTF());
      }
   }

}
