package org.infinispan.client.hotrod.exceptions;

import java.net.SocketAddress;

/**
 * Indicates a communication exception with the Hot Rod server: e.g. TCP connection is broken while reading a response
 * from the server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportException extends HotRodClientException {

   private final SocketAddress serverAddress;

   public TransportException(String message, SocketAddress serverAddress) {
      super(message);
      this.serverAddress = serverAddress;
   }

   public TransportException(String message, Throwable cause, SocketAddress serverAddress) {
      super(message, cause);
      this.serverAddress = serverAddress;
   }

   public TransportException(Throwable cause, SocketAddress serverAddress) {
      super(cause);
      this.serverAddress = serverAddress;
   }

   public SocketAddress getServerAddress() {
      return serverAddress;
   }

}
