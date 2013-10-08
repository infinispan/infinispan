package org.infinispan.lucene.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies that the Lucene Directory correctly refuses to use a Metadata Cache with eviction or with persistence
 * without preload enabled.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lucene.configuration.MetadataCacheValidationTest")
public class MetadataCacheValidationTest extends AbstractInfinispanTest {

   private static final String INDEX_NAME = "test-index";
   private static final String CACHE_NAME = "test-cache";

   @Test(expectedExceptions = IllegalArgumentException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Lucene Directory for index '" + INDEX_NAME +
               "' can not use Metadata Cache '" + CACHE_NAME + "': eviction enabled on the Cache configuration!")
   public void testFailOnEviction() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.eviction().strategy(EvictionStrategy.LIRS).maxEntries(1);
      doConfigurationTest(builder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Lucene Directory for index '" + INDEX_NAME +
               "' can not use Metadata Cache '" + CACHE_NAME + "': persistence enabled without preload on the Cache " +
               "configuration!")
   public void testFailOnPersistenceWithoutPreload() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(false);
      doConfigurationTest(builder);
   }

   public void testSuccessfullConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true);
      builder.eviction().strategy(EvictionStrategy.NONE).maxEntries(-1);
      doConfigurationTest(builder);
   }

   private void doConfigurationTest(ConfigurationBuilder configuration) {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = TestCacheManagerFactory.createCacheManager(configuration);
         Cache cache = cacheManager.getCache(CACHE_NAME);

         DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).create();
      } finally {
         if (cacheManager != null) {
            TestingUtil.killCacheManagers(cacheManager);
         }
      }
   }


}
