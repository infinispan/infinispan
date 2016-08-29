package org.infinispan.objectfilter;

/**
 * Thrown in case of syntax errors during parsing or during the processing of the parse tree.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class ParsingException extends RuntimeException {

   public ParsingException(String message) {
      super(message);
   }

   public ParsingException(Throwable cause) {
      super(cause);
   }

   public ParsingException(String message, Throwable cause) {
      super(message, cause);
   }
}
