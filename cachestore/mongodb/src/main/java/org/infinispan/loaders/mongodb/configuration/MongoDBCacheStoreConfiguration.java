package org.infinispan.loaders.mongodb.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.mongodb.MongoDBCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

/**
 * Contains the hole cachestore configuration at the runtime
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@BuiltBy(MongoDBCacheStoreConfigurationBuilder.class)
public class MongoDBCacheStoreConfiguration extends AbstractStoreConfiguration implements LegacyLoaderAdapter<MongoDBCacheStoreConfig> {

   private final String host;
   private final int port;
   private final int timeout;
   private final String username;
   private final String password;
   private final String database;
   private final String collection;
   private final int acknowledgment;

   MongoDBCacheStoreConfiguration(String host, int port, int timeout, String username, String password, String database, String collection, int acknowledgment, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
                                  boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
                                  AsyncStoreConfiguration asyncStoreConfiguration, SingletonStoreConfiguration singletonStoreConfiguration) {
      super(purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications, properties,
            asyncStoreConfiguration, singletonStoreConfiguration);
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.database = database;
      this.collection = collection;
      this.timeout = timeout;
      this.acknowledgment = acknowledgment;
   }

   @Override
   public MongoDBCacheStoreConfig adapt() {
      MongoDBCacheStoreConfig mongoDBCacheStoreConfig = new MongoDBCacheStoreConfig(host, port, timeout, username, password, database, collection, acknowledgment);
      LegacyConfigurationAdaptor.adapt(this, mongoDBCacheStoreConfig);
      return mongoDBCacheStoreConfig;
   }

   public String host() {
      return this.host;
   }

   public int port() {
      return this.port;
   }

   public int timeout() {
      return this.timeout;
   }

   public String username() {
      return this.username;
   }

   public String password() {
      return this.password;
   }

   public String database() {
      return this.database;
   }

   public String collection() {
      return this.collection;
   }

   public int acknowledgment() {
      return this.acknowledgment;
   }
}