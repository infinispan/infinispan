package org.infinispan.remoting.responses;

import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.Ids;

import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * An unsure response - used with Dist - essentially asks the caller to check the next response from the next node since
 * the sender is in a state of flux (probably in the middle of rebalancing)
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = UnsureResponse.Externalizer.class, id = Ids.UNSURE_RESPONSE)
public class UnsureResponse extends ValidResponse {
   public static final UnsureResponse INSTANCE = new UnsureResponse();
   public boolean isSuccessful() {
      return false;
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }   
}
