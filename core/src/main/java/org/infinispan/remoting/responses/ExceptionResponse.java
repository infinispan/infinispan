package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

/**
 * A response that encapsulates an exception
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ExceptionResponse.Externalizer.class, id = Ids.EXCEPTION_RESPONSE)
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
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         output.writeObject(((ExceptionResponse) subject).exception);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ExceptionResponse((Exception) input.readObject());
      }
   }
}
