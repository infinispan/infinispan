package org.infinispan.loaders.mongodb.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.commons.util.TypedProperties;

/**
 * MongoDBCacheStoreConfigurationBuilder. Configures a {@link MongoDBCacheStoreConfiguration}
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public class MongoDBCacheStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<MongoDBCacheStoreConfiguration, MongoDBCacheStoreConfigurationBuilder> {

   private String host;
   private int port;
   private int timeout;
   private String username;
   private String password;
   private String database;
   private String collection;
   private int acknowledgment;

   public MongoDBCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      super.fetchPersistentState = true;
      super.purgeOnStartup = true;
      super.ignoreModifications = true;
      super.async.enable();
   }

   @Override
   public MongoDBCacheStoreConfiguration create() {
      return new MongoDBCacheStoreConfiguration(host, port, timeout, username, password, database, collection, acknowledgment,
                                                purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState,
                                                ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public MongoDBCacheStoreConfigurationBuilder read(MongoDBCacheStoreConfiguration template) {
      this.host = template.host();
      this.username = template.username();
      this.password = template.password();
      this.database = template.database();
      this.collection = template.collection();
      this.timeout = template.timeout();
      this.acknowledgment = template.acknowledgment();
      super.purgeOnStartup = template.purgeOnStartup();
      super.purgeSynchronously = template.purgeSynchronously();
      super.purgerThreads = template.purgerThreads();
      super.fetchPersistentState = template.fetchPersistentState();
      super.ignoreModifications = template.ignoreModifications();
      return this;
   }

   @Override
   public MongoDBCacheStoreConfigurationBuilder self() {
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder timeout(int timeout) {
      this.timeout = timeout;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder username(String username) {
      this.username = username;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder password(String password) {
      this.password = password;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder database(String database) {
      this.database = database;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder collection(String collection) {
      this.collection = collection;
      return this;
   }

   public MongoDBCacheStoreConfigurationBuilder acknowledgment(int acknowledgment) {
      this.acknowledgment = acknowledgment;
      return this;
   }
}
