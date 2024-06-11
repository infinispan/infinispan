package org.infinispan.client.hotrod.configuration;

/**
 * ServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ServerConfiguration {
   private final String host;
   private final int port;

   ServerConfiguration(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public String host() {
      return host;
   }

   public int port() {
      return port;
   }

   @Override
   public String toString() {
      return "ServerConfiguration[" +
            "host='" + host + '\'' +
            ", port=" + port +
            ']';
   }
}
