package org.infinispan.server.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.junit4.DatabaseServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Persistence.class)
public class ManagedConnectionOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = PersistenceIT.SERVERS;

   @ClassRule
   public static DatabaseServerRule DATABASE = new DatabaseServerRule(SERVERS);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   public ManagedConnectionOperations() {
      DATABASE.setDatabaseType("h2");
   }

   private org.infinispan.configuration.cache.ConfigurationBuilder createConfigurationBuilder() {

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .table()
            .dropOnExit(true)
            .tableNamePrefix("TBL")
            .idColumnName("ID").idColumnType(DATABASE.getDatabase().getIdColumType())
            .dataColumnName("DATA").dataColumnType(DATABASE.getDatabase().getDataColumnType())
            .timestampColumnName("TS").timestampColumnType(DATABASE.getDatabase().getTimeStampColumnType())
            .segmentColumnName("S").segmentColumnType(DATABASE.getDatabase().getSegmentColumnType())
            .dataSource().jndiUrl("jdbc/database");
      return builder;
   }

   @Test
   public void testTwoCachesSameCacheStore() {
      RemoteCache<String, String> cache1 = SERVER_TEST.hotrod().withServerConfiguration(createConfigurationBuilder()).withQualifier("1").create();
      RemoteCache<String, String> cache2 = SERVER_TEST.hotrod().withServerConfiguration(createConfigurationBuilder()).withQualifier("2").create();
      cache1.put("k1", "v1");
      String firstK1 = cache1.get("k1");
      assertEquals("v1", firstK1);
      assertNull(cache2.get("k1"));
      cache2.put("k2", "v2");
      assertEquals("v2", cache2.get("k2"));
      assertNull(cache1.get("k2"));

      assertCleanCacheAndStore(cache1);
      assertCleanCacheAndStore(cache2);
   }

   @Test
   public void testPutGetRemove() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withServerConfiguration(createConfigurationBuilder()).create();
      cache.put("k1", "v1");
      cache.put("k2", "v2");

      assertNotNull(cache.get("k1"));
      assertNotNull(cache.get("k2"));

      cache.stop();
      cache.start();

      assertNotNull(cache.get("k1"));
      assertNotNull(cache.get("k2"));
      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));
      cache.remove("k1");
      assertNull(cache.get("k1"));
   }

   protected void assertCleanCacheAndStore(RemoteCache cache) {
      cache.clear();
      assertEquals(0, cache.size());
   }

}
