package org.infinispan.loaders.mongodb.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@Test(groups = "unit", testName = "loaders.mongodb.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testMongoDBConfigurationBuilder() {
      final String host = "localhost";
      final int port = 27017;
      final int timeout = 1500;
      final String username = "mongoDBUSer";
      final String password = "mongoBDPassword";
      final String database = "infinispan_cachestore";
      final String collection = "entries";

      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(MongoDBCacheStoreConfigurationBuilder.class)
            .host(host)
            .port(port)
            .timeout(timeout)
            .acknowledgment(0)
            .username(username)
            .password(password)
            .database(database)
            .collection(collection);

      final Configuration config = b.build();
      MongoDBCacheStoreConfiguration store = (MongoDBCacheStoreConfiguration) config.loaders().cacheLoaders().get(0);

      assertEquals(store.host(), host);
      assertEquals(store.port(), port);
      assertEquals(store.timeout(), timeout);
      assertEquals(store.username(), username);
      assertEquals(store.password(), password);
      assertEquals(store.database(), database);
      assertEquals(store.collection(), collection);
   }
}
