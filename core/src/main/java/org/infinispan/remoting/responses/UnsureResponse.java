package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * An unsure response - used with Dist - essentially asks the caller to check the next response from the next node since
 * the sender is in a state of flux (probably in the middle of rebalancing)
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsureResponse extends ValidResponse {
   public static final UnsureResponse INSTANCE = new UnsureResponse();
   @Override
   public boolean isSuccessful() {
      return false;
   }

   @Override
   public Object getResponseValue() {
      throw new UnsupportedOperationException();
   }

   public static class Externalizer extends AbstractExternalizer<UnsureResponse> {
      @Override
      public void writeObject(ObjectOutput output, UnsureResponse subject) throws IOException {
      }

      @Override
      public UnsureResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }

      @Override
      public Integer getId() {
         return Ids.UNSURE_RESPONSE;
      }

      @Override
      public Set<Class<? extends UnsureResponse>> getTypeClasses() {
         return Util.<Class<? extends UnsureResponse>>asSet(UnsureResponse.class);
      }
   }
}
