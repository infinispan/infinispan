package org.infinispan.loaders.cassandra.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.CacheConfigurationException;

/**
 *
 * CassandraServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CassandraServerConfigurationBuilder extends AbstractCassandraCacheStoreConfigurationChildBuilder<CassandraCacheStoreConfigurationBuilder> implements Builder<CassandraServerConfiguration> {

   protected CassandraServerConfigurationBuilder(CassandraCacheStoreConfigurationBuilder builder) {
      super(builder);
   }

   private String host;
   private int port = 9160;

   public CassandraServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public CassandraServerConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public void validate() {
      if (host==null) {
         throw new CacheConfigurationException("Missing host");
      }
   }

   @Override
   public CassandraServerConfiguration create() {
      return new CassandraServerConfiguration(host, port);
   }

   @Override
   public CassandraServerConfigurationBuilder read(CassandraServerConfiguration template) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }

}
