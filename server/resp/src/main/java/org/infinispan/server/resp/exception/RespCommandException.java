package org.infinispan.server.resp.exception;

/**
 * Represent a command failure.
 *
 * <p>
 * This exception holds the error message for the command and represents the command failure. It does not collect the stack
 * trace. This is useful when the command failed and has an expected error message. For example, passing something that is
 * not a number to a command.
 * </p>
 *
 * @since 16.0
 * @author José Bolina
 */
public final class RespCommandException extends RuntimeException {

   public RespCommandException(String message) {
      super(message, null, true, false);
   }
}
