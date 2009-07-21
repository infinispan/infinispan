package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = UnsuccessfulResponse.Externalizer.class, id = Ids.UNSUCCESSFUL_RESPONSE)
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse INSTANCE = new UnsuccessfulResponse();

   private UnsuccessfulResponse() {
   }

   public boolean isSuccessful() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      return o.getClass().equals(UnsuccessfulResponse.class);
   }

   @Override
   public int hashCode() {
      return 13;
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         // no-op
      }
      
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }
}
