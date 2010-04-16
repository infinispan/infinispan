package org.infinispan.client.hotrod.exceptions;

import org.infinispan.client.hotrod.exceptions.HotRodException;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class TransportException extends HotRodException {
   public TransportException() {
   }

   public TransportException(String message) {
      super(message);
   }

   public TransportException(String message, Throwable cause) {
      super(message, cause);
   }

   public TransportException(Throwable cause) {
      super(cause);
   }
}