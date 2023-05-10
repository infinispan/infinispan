package org.infinispan.server.hotrod;

import java.net.InetAddress;

/**
 * A Hot Rod server address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface ServerAddress {

   /**
    * Returns the mapping for the
    * @param localAddress
    * @return
    */
   String getHost(InetAddress localAddress);

   int getPort();

   static ServerAddress forAddress(String host, int port, boolean networkPrefixOverride) {
      if ("0.0.0.0".equals(host) || "0:0:0:0:0:0:0:0".equals(host)) {
         return new MultiHomedServerAddress(port, networkPrefixOverride);
      } else {
         return new SingleHomedServerAddress(host, port);
      }
   }

}
