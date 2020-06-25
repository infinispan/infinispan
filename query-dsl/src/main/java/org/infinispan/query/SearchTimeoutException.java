package org.infinispan.query;

/**
 * Thrown when a query timeout occurs.
 *
 * @since 11.0
 */
public class SearchTimeoutException extends RuntimeException {

   public SearchTimeoutException() {
   }

   public SearchTimeoutException(String msg) {
      super(msg);
   }
}
