package org.infinispan.all.embeddedquery;

import org.infinispan.all.embeddedquery.testdomain.Person;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// TODO [anistor] remove this test in infinispan 10.0
/**
 * Tests that undeclared indexed entities are autodetected.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public class IndexedEntityAutodetectTest extends LocalCacheTest {
   protected static EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcfg = new GlobalConfigurationBuilder();

      // this configuration does not declare any indexed types on purpose, so they are autodetected
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.all.embeddedquery.testdomain.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager cm = new DefaultCacheManager(gcfg.build(), cfg.build());

      return cm;
   }

   @Before
   public void init() throws Exception {
      cacheManager = createCacheManager();
      cache = cacheManager.getCache();
   }

   @After
   public void tearDown() {
      if (cacheManager != null) {
         cacheManager.stop();
      }
   }

   @Override
   protected void loadTestingData() {
      assertIndexingKnows(cache);

      super.loadTestingData();

      assertIndexingKnows(cache, Person.class);
   }

   @Test
   public void testEntityDiscovery() {
      assertIndexingKnows(cache);

      Person p = new Person();
      p.setName("Lucene developer");
      p.setAge(30);
      p.setBlurb("works best on weekends");
      cache.put(p.getName(), p);

      assertIndexingKnows(cache, Person.class);
   }
}
