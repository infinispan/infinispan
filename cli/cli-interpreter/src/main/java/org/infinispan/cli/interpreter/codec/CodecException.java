package org.infinispan.cli.interpreter.codec;

import org.infinispan.cli.interpreter.result.StatementException;

/**
 * CodecException.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CodecException extends StatementException {

   public CodecException(Throwable cause) {
      super(cause);
   }

   public CodecException(String message) {
      super(message);
   }

   public CodecException(String message, Throwable cause) {
      super(message, cause);
   }
}
