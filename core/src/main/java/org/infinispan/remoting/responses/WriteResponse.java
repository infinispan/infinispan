package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.marshall.core.Ids;

/**
 * The primary owner {@link Response} with the return value.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class WriteResponse implements Response {

   public static final AdvancedExternalizer<WriteResponse> EXTERNALIZER = new Externalizer();
   private final Object returnValue;
   private final boolean commandSuccessful;

   public WriteResponse(Object returnValue, boolean commandSuccessful) {
      this.returnValue = returnValue;
      this.commandSuccessful = commandSuccessful;
   }

   public Object getReturnValue() {
      return returnValue;
   }

   public boolean isCommandSuccessful() {
      return commandSuccessful;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isValid() {
      return true;
   }

   @Override
   public String toString() {
      return "WriteResponse{" +
            "returnValue=" + returnValue +
            ", commandSuccessful=" + commandSuccessful +
            '}';
   }

   private static class Externalizer extends AbstractExternalizer<WriteResponse> {
      @Override
      public void writeObject(ObjectOutput output, WriteResponse response) throws IOException {
         output.writeObject(response.returnValue);
         output.writeBoolean(response.commandSuccessful);
      }

      @Override
      public WriteResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new WriteResponse(input.readObject(), input.readBoolean());
      }

      @Override
      public Integer getId() {
         return Ids.WRITE_RESPONSE;
      }

      @Override
      public Set<Class<? extends WriteResponse>> getTypeClasses() {
         return Collections.singleton(WriteResponse.class);
      }
   }
}
