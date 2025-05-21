package org.infinispan.server.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Properties;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.server.test.core.AeshTestConnection;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.category.Persistence;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Category(Persistence.class)
public class ManagedConnectionOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = PersistenceIT.SERVERS;

   private org.infinispan.configuration.cache.ConfigurationBuilder createConfigurationBuilder(Database database) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .table()
            .dropOnExit(true)
            .tableNamePrefix("TBL")
            .idColumnName("ID").idColumnType(database.getIdColumType())
            .dataColumnName("DATA").dataColumnType(database.getDataColumnType())
            .timestampColumnName("TS").timestampColumnType(database.getTimeStampColumnType())
            .segmentColumnName("S").segmentColumnType(database.getSegmentColumnType())
            .dataSource().jndiUrl("jdbc/" + database.getType());
      return builder;
   }

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testTwoCachesSameCacheStore(Database database) {
      RemoteCache<String, String> cache1 = SERVERS.hotrod().withServerConfiguration(createConfigurationBuilder(database)).withQualifier("1").create();
      RemoteCache<String, String> cache2 = SERVERS.hotrod().withServerConfiguration(createConfigurationBuilder(database)).withQualifier("2").create();
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

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   public void testPutGetRemove(Database database) {
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(createConfigurationBuilder(database)).create();
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

   @ParameterizedTest
   @ArgumentsSource(Common.DatabaseProvider.class)
   @Tag("cli")
   public void testDataSourceCLI(Database database) {
      try (AeshTestConnection terminal = new AeshTestConnection()) {
         CLI.main(new AeshDelegatingShell(terminal), new String[]{}, new Properties());
         terminal.send("connect " + SERVERS.getServerDriver().getServerAddress(0).getHostAddress() + ":11222");
         terminal.assertContains("//containers/default]>");
         terminal.clear();
         terminal.send("server datasource ls");
         terminal.assertContains(database.getType());
         terminal.clear();
         terminal.send("server datasource test " + database.getType());
         terminal.assertContains("ISPN012502: Connection to data source '" + database.getType()+ "' successful");
      }
   }
}
