package org.infinispan.loaders.cassandra.configuration;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;
import org.infinispan.loaders.keymappers.Key2StringMapper;

/**
 * AbstractCassandraCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractCassandraCacheStoreConfigurationChildBuilder<T> extends
      AbstractStoreConfigurationChildBuilder<T> implements CassandraCacheStoreConfigurationChildBuilder<T> {

   private CassandraCacheStoreConfigurationBuilder builder;

   protected AbstractCassandraCacheStoreConfigurationChildBuilder(CassandraCacheStoreConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace) {
      return builder.autoCreateKeyspace(autoCreateKeyspace);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder configurationPropertiesFile(String configurationPropertiesFile) {
      return builder.configurationPropertiesFile(configurationPropertiesFile);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily) {
      return builder.entryColumnFamily(entryColumnFamily);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily) {
      return builder.expirationColumnFamily(expirationColumnFamily);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder framed(boolean framed) {
      return builder.framed(framed);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder keyMapper(String keyMapper) {
      return builder.keyMapper(keyMapper);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder keyMapper(Class<? extends Key2StringMapper> keyMapper) {
      return builder.keyMapper(keyMapper);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder keySpace(String keySpace) {
      return builder.keySpace(keySpace);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder password(String password) {
      return builder.password(password);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel) {
      return builder.readConsistencyLevel(readConsistencyLevel);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder username(String username) {
      return builder.username(username);
   }

   @Override
   public CassandraCacheStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel) {
      return builder.writeConsistencyLevel(writeConsistencyLevel);
   }

   @Override
   public CassandraServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

}
