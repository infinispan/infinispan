package org.infinispan.cli.interpreter.result;

public class StatementException extends Exception {

   public StatementException(Throwable cause) {
      super(cause);
   }

   public StatementException(String message) {
      super(message);
   }

   public StatementException(String message, Throwable cause) {
      super(message, cause);
   }

}
