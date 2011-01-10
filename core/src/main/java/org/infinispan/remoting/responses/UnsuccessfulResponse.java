package org.infinispan.remoting.responses;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

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

   public static class Externalizer extends AbstractExternalizer<UnsuccessfulResponse> {
      @Override
      public void writeObject(ObjectOutput output, UnsuccessfulResponse object) throws IOException {
         // no-op
      }
      
      @Override
      public UnsuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }

      @Override
      public Integer getId() {
         return Ids.UNSUCCESSFUL_RESPONSE;
      }

      @Override
      public Set<Class<? extends UnsuccessfulResponse>> getTypeClasses() {
         return Util.<Class<? extends UnsuccessfulResponse>>asSet(UnsuccessfulResponse.class);
      }
   }
}
