package org.infinispan.query.config;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * @author gustavonalle
 * @since 7.0
 */
public class ProgrammaticAutoConfigTest {

   @Test
   public void testWithoutAutoConfig() {
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .indexing().index(Index.ALL).create();
      
      assertTrue(cfg.properties().isEmpty());
   }

   @Test
   public void testLocalWitAutoConfig() {
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .indexing().index(Index.ALL).autoConfig(true)
              .create();

      assertFalse(cfg.properties().isEmpty());
      assertEquals(cfg.properties().get("hibernate.search.default.directory_provider"), "filesystem");
   }

   @Test
   public void testDistWitAutoConfig() {
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .clustering().cacheMode(CacheMode.DIST_SYNC)
              .indexing().index(Index.ALL).autoConfig(true)
              .create();

      assertFalse(cfg.properties().isEmpty());
      assertEquals(cfg.properties().get("hibernate.search.default.directory_provider"), "infinispan");
   }

   @Test
   public void testOverride() {
      String override = "hibernate.search.default.exclusive_index_use";
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .indexing()
              .index(Index.ALL)
              .autoConfig(true)
              .addProperty(override, "false").create();

      assertEquals(cfg.properties().get(override), "false");
   }


}
