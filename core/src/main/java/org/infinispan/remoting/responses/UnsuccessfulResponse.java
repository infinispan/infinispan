package org.infinispan.remoting.responses;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshalls;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse INSTANCE = new UnsuccessfulResponse();

   private UnsuccessfulResponse() {
   }

   public boolean isSuccessful() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null) return false;
      return o.getClass().equals(this.getClass());
   }

   @Override
   public int hashCode() {
      return 13;
   }

   @Marshalls(typeClasses = UnsuccessfulResponse.class, id = Ids.UNSUCCESSFUL_RESPONSE)
   public static class Externalizer implements org.infinispan.marshall.Externalizer<UnsuccessfulResponse> {
      public void writeObject(ObjectOutput output, UnsuccessfulResponse object) throws IOException {
         // no-op
      }
      
      public UnsuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }
}
