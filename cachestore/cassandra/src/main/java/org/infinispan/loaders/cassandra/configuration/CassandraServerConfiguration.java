package org.infinispan.loaders.cassandra.configuration;

/**
 * CassandraServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CassandraServerConfiguration {

   private final String host;
   private final int port;

   CassandraServerConfiguration(String host, int port) {
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
