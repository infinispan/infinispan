package org.infinispan.server.test.client.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

/**
 * @author gustavonalle
 * @since 8.0
 */
@SerializeWith(Summary.SummaryExternalizer.class)
public class Summary {

   private final List<String> attributes;

   public Summary(List<String> attributes) {
      this.attributes = attributes;
   }

   public List<String> getAttributes() {
      return attributes;
   }

   public static class SummaryExternalizer implements Externalizer<Summary> {

      @Override
      public void writeObject(ObjectOutput output, Summary object) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, object.getAttributes().size());
         for (String attribute : object.getAttributes()) {
            output.writeUTF(attribute);
         }
      }

      @Override
      public Summary readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int size = UnsignedNumeric.readUnsignedInt(input);
         ArrayList<String> attributes = new ArrayList<>(size);
         for (int i = 0; i < size; i++) {
            attributes.add(input.readUTF());
         }
         return new Summary(attributes);
      }
   }
}
