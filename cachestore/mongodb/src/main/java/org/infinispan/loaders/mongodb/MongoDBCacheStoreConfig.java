package org.infinispan.loaders.mongodb;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.mongodb.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configures {@link MongoDBCacheStore}.
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public class MongoDBCacheStoreConfig extends LockSupportCacheStoreConfig {
   private static final Log log = LogFactory.getLog(MongoDBCacheStoreConfig.class, Log.class);
   private String host;
   private int port;
   private int timeout;
   private String username;
   private String password;
   private String database;
   private String collection;
   private int acknowledgment;

   public MongoDBCacheStoreConfig() {
      super.setCacheLoaderClassName(MongoDBCacheStore.class.getName());
   }

   public MongoDBCacheStoreConfig(String host, int port, int timeout, String username, String password, String database, String collection, int acknowledgment) {
      if (port < 1 || port > 65535) {
         log.mongoPortIllegalValue(port);
      }
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.database = database;
      this.collection = collection;
      this.timeout = timeout;
      if (acknowledgment <= -1) {
         this.acknowledgment = -1;
      } else if (this.acknowledgment >= 2) {
         this.acknowledgment = 2;
      } else {
         this.acknowledgment = acknowledgment;
      }
   }

   public String getHost() {
      return host;
   }

   public int getPort() {
      return port;
   }

   public String getUsername() {
      return username;
   }

   public String getPassword() {
      return password;
   }

   public String getDatabase() {
      return database;
   }

   public String getCollectionName() {
      return collection;
   }

   public int getTimeout() {
      return timeout;
   }

   public int getAcknowledgment() {
      return acknowledgment;
   }
}