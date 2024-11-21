package org.infinispan.client.hotrod.exceptions;

/**
 * @author Guillaume Darmont / guillaume@dropinocean.com
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
