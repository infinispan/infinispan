package org.infinispan.remoting.responses;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;

public class BiasRevocationResponse extends SuccessfulResponse {
   private final Address[] waitFor;

   public BiasRevocationResponse(Object responseValue, Address[] waitFor) {
      super(responseValue);
      this.waitFor = waitFor;
   }

   public Address[] getWaitList() {
      return waitFor;
   }

   public static class Externalizer implements AdvancedExternalizer<BiasRevocationResponse> {
      @Override
      public Set<Class<? extends BiasRevocationResponse>> getTypeClasses() {
         return Util.asSet(BiasRevocationResponse.class);
      }

      @Override
      public Integer getId() {
         return Ids.BIAS_REVOCATION_RESPONSE;
      }

      @Override
      public void writeObject(UserObjectOutput output, BiasRevocationResponse object) throws IOException {
         output.writeObject(object.getResponseValue());
         MarshallUtil.marshallArray(object.waitFor, output);
      }

      @Override
      public BiasRevocationResponse readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         Object value = input.readObject();
         Address[] waitFor = MarshallUtil.unmarshallArray(input, Address[]::new);
         return new BiasRevocationResponse(value, waitFor);
      }
   }
}
