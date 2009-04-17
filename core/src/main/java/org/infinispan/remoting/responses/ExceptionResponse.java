package org.infinispan.remoting.responses;

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
}
