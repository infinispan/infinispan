package org.infinispan.client.hotrod.exceptions;

/**
 * Indicates a communication exception with the hotrod server: e.g. TCP connection is broken while reading a response
 * from the server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportException extends HotRodClientException {
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