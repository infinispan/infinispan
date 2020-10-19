package org.infinispan.query;

import org.infinispan.commons.TimeoutException;

/**
 * Thrown when a query timeout occurs.
 *
 * @since 11.0
 */
public class SearchTimeoutException extends TimeoutException {

   public SearchTimeoutException() {
   }

   public SearchTimeoutException(String msg) {
      super(msg);
   }
}
