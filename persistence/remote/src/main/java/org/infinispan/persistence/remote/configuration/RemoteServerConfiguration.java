package org.infinispan.persistence.remote.configuration;

public class RemoteServerConfiguration {
   private final String host;
   private final int port;

   RemoteServerConfiguration(String host, int port) {
      this.host = host;
      this.port = port;
   }

   public String host() {
      return host;
   }

   public int port() {
      return port;
   }

}
