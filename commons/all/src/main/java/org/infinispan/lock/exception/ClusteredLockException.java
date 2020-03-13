package org.infinispan.lock.exception;

/**
 * Exception used to handle errors on clustered locks
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class ClusteredLockException extends RuntimeException {

   public ClusteredLockException(String message) {
      super(message);
   }

   public ClusteredLockException(Throwable t) {
      super(t);
   }
}
