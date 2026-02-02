package org.infinispan.server.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.test.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@org.infinispan.server.test.core.tags.Database
public class BaseJdbcStringBasedCacheStore {

   @InfinispanServer(PersistenceIT.class)
   public static TestClientDriver SERVERS;

   /*
    * This should verify that DefaultTwoWayKey2StringMapper on server side can work with ByteArrayKey which
    * is always produced by HotRod client regardless of type of key being stored in a cache.
    */
   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testDefaultTwoWayKey2StringMapper(Database database) {
      JdbcConfigurationUtil jdbcUtil = new JdbcConfigurationUtil(CacheMode.REPL_SYNC, database, false, true)
            .setLockingConfigurations();
      RemoteCache<Object, Object> cache = SERVERS.hotrod()
            .withClientConfiguration(database.getHotrodClientProperties())
            .withServerConfiguration(jdbcUtil.getConfigurationBuilder()).create();
      try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration())) {
         Double doubleKey = 10.0;
         Double doubleValue = 20.0;
         assertEquals(0, cache.size());
         assertEquals(0, table.countAllRows());
         cache.put(doubleKey, doubleValue);
         // test passivation==false, database should contain all entries which are in the cache
         assertEquals(1, table.countAllRows());
         assertEquals(doubleValue, cache.get(doubleKey));
      }
   }

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testExpiration(Database database) {
      var jdbcUtil = new JdbcConfigurationUtil(CacheMode.LOCAL, database, false, false);
      var configBuilder = jdbcUtil.getConfigurationBuilder();
      configBuilder.expiration()
            .lifespan(1)
            .wakeUpInterval("10ms");
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withClientConfiguration(database.getHotrodClientProperties())
            .withServerConfiguration(configBuilder).create();
      cache.put("Key", "Value");
      Eventually.eventually(cache::isEmpty);
      try(TableManipulation table = new TableManipulation(cache.getName(), jdbcUtil.getPersistenceConfiguration())) {
         table.countAllRows();
         Eventually.eventually(() -> {
            var rows = table.countAllRows();
            System.out.println(rows);
            return rows == 0;
         });
      }
   }
}
