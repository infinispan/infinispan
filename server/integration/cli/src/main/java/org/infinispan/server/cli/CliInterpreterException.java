package org.infinispan.server.cli;

/**
 * The {@link java.lang.Exception} thrown when the Infinispan CLI interpreter returns an error message. The error
 * message is set in the {@code Exception} message.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CliInterpreterException extends Exception {

   public CliInterpreterException(String message, Throwable cause) {
      super(message, cause);
   }

   public CliInterpreterException(String message) {
      super(message);
   }

   public CliInterpreterException(Throwable cause) {
      super(cause);
   }
}
