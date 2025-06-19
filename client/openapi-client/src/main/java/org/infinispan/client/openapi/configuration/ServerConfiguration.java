package org.infinispan.client.openapi.configuration;

/**
 * ServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 16.0
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
