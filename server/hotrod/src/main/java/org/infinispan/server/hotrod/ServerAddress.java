package org.infinispan.server.hotrod;

import java.net.InetAddress;

import org.infinispan.remoting.transport.Address;

/**
 * A Hot Rod server address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface ServerAddress extends Address {

   /**
    * Returns the mapping for the
    * @param localAddress
    * @return
    */
   String getHost(InetAddress localAddress);

   int getPort();

   static ServerAddress forAddress(String host, int port) {
      if ("0.0.0.0".equals(host) || "::0".equals(host)) {
         return new MultiHomedServerAddress(port);
      } else {
         return new SingleHomedServerAddress(host, port);
      }
   }

}
