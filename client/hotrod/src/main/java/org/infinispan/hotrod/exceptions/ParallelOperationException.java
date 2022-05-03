package org.infinispan.hotrod.exceptions;

/**
 */
public class ParallelOperationException extends HotRodClientException {

   public ParallelOperationException(String message) {
      super(message);
   }

   public ParallelOperationException(Throwable cause) {
      super(cause);
   }

   public ParallelOperationException(String message, Throwable cause) {
      super(message, cause);
   }
}
