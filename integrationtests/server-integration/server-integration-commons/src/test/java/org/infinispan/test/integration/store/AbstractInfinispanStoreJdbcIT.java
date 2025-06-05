package org.infinispan.test.integration.store;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.junit.After;
import org.junit.Test;

/**
 * Test the Infinispan JDBC CacheStore AS module integration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractInfinispanStoreJdbcIT {

   private EmbeddedCacheManager cm;

   @After
   public void cleanUp() {
      if (cm != null)
         cm.stop();
   }

   @Test
   public void testCacheManager() {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.getGlobalConfigurationBuilder().defaultCacheName("default");
      ConfigurationBuilder builder = holder.newConfigurationBuilder("default");
      builder.persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class)
            .table()
            .tableNamePrefix("ISPN")
            .idColumnName("K")
            .idColumnType("VARCHAR(255)")
            .dataColumnName("V")
            .dataColumnType("BLOB")
            .timestampColumnName("T")
            .timestampColumnType("BIGINT")
            .segmentColumnName("S")
            .segmentColumnType("BIGINT")
            .dataSource().jndiUrl(System.getProperty("infinispan.server.integration.data-source"));

      cm = new DefaultCacheManager(holder);

      Cache<String, String> cache = cm.getCache();
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }

   @Test
   public void testXmlConfig() throws IOException {
      cm = new DefaultCacheManager("jdbc-config.xml");
      Cache<String, String> cache = cm.getCache("anotherCache");
      cache.put("a", "a");
      assertEquals("a", cache.get("a"));
   }

}
