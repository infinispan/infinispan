package org.infinispan.lucene;

import java.io.IOException;

/**
 * Thrown when a lock is in a unexpected state.
 *
 * @since 9.0
 */
public class InvalidLockException extends IOException {

   public InvalidLockException(String message) {
      super(message);
   }

}
