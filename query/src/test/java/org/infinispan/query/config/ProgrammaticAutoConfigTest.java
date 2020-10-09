package org.infinispan.query.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "unit", testName = "query.config.ProgrammaticAutoConfigTest")
public class ProgrammaticAutoConfigTest {

   @Test
   public void testWithoutAutoConfig() {
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .indexing().enable().create();

      assertTrue(cfg.properties().isEmpty());
   }

   @Test
   public void testLocalWitAutoConfig() {
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .indexing().enable().autoConfig(true)
              .create();

      assertFalse(cfg.properties().isEmpty());
      assertEquals(cfg.properties().get("hibernate.search.backend.directory.type"), "local-filesystem");
   }

   @Test
   public void testDistWitAutoConfig() {
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .clustering().cacheMode(CacheMode.DIST_SYNC)
              .indexing().enable().autoConfig(true)
              .create();

      assertFalse(cfg.properties().isEmpty());
      assertEquals(cfg.properties().get("hibernate.search.backend.directory.type"), "local-filesystem");
   }

   @Test
   public void testOverride() {
      String override = "hibernate.search.default.exclusive_index_use";
      IndexingConfiguration cfg = new ConfigurationBuilder()
              .indexing()
              .enable()
              .autoConfig(true)
              .addProperty(override, "false").create();

      assertEquals(cfg.properties().get(override), "false");
   }


}
