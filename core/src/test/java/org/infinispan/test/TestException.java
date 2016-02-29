package org.infinispan.test;

/**
 * Well-known exception used for testing exception propagation.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class TestException extends RuntimeException {
   public TestException() {
   }

   public TestException(String message) {
      super(message);
   }

   public TestException(String message, Throwable cause) {
      super(message, cause);
   }

   public TestException(Throwable cause) {
      super(cause);
   }
}
