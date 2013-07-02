package org.infinispan.loaders.cassandra.configuration;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;
import org.infinispan.loaders.keymappers.Key2StringMapper;

/**
 * CassandraCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface CassandraCacheStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {
   CassandraCacheStoreConfigurationBuilder autoCreateKeyspace(boolean autoCreateKeyspace);

   CassandraCacheStoreConfigurationBuilder configurationPropertiesFile(String configurationPropertiesFile);

   CassandraCacheStoreConfigurationBuilder entryColumnFamily(String entryColumnFamily);

   CassandraCacheStoreConfigurationBuilder expirationColumnFamily(String expirationColumnFamily);

   CassandraCacheStoreConfigurationBuilder framed(boolean framed);

   CassandraCacheStoreConfigurationBuilder keyMapper(String keyMapper);

   CassandraCacheStoreConfigurationBuilder keyMapper(Class<? extends Key2StringMapper> keyMapper);

   CassandraCacheStoreConfigurationBuilder keySpace(String keySpace);

   CassandraCacheStoreConfigurationBuilder password(String password);

   CassandraCacheStoreConfigurationBuilder readConsistencyLevel(ConsistencyLevel readConsistencyLevel);

   CassandraCacheStoreConfigurationBuilder username(String username);

   CassandraCacheStoreConfigurationBuilder writeConsistencyLevel(ConsistencyLevel writeConsistencyLevel);

   CassandraServerConfigurationBuilder addServer();
}
