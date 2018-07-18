package org.infinispan.remoting.responses;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * A response that encapsulates an exception
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ExceptionResponse extends InvalidResponse {
   private Exception exception;

   public ExceptionResponse() {
   }

   public ExceptionResponse(Exception exception) {
      this.exception = exception;
   }

   public Exception getException() {
      return exception;
   }

   public void setException(Exception exception) {
      this.exception = exception;
   }

   @Override
   public String toString() {
      return "ExceptionResponse(" + exception + ")";
   }

   public static class Externalizer extends AbstractExternalizer<ExceptionResponse> {
      @Override
      public void writeObject(UserObjectOutput output, ExceptionResponse response) throws IOException {
         output.writeObject(response.exception);
      }

      @Override
      public ExceptionResponse readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new ExceptionResponse((Exception) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.EXCEPTION_RESPONSE;
      }

      @Override
      public Set<Class<? extends ExceptionResponse>> getTypeClasses() {
         return Util.<Class<? extends ExceptionResponse>>asSet(ExceptionResponse.class);
      }
   }
}
