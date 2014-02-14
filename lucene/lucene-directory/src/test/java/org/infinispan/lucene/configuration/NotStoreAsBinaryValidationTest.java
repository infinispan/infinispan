package org.infinispan.lucene.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies that the Lucene Directory correctly refuses to use a Cache with store as binary enabled.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lucene.configuration.NotStoreAsBinaryValidationTest")
public class NotStoreAsBinaryValidationTest extends AbstractInfinispanTest {

   private static final String INDEX_NAME = "test-index";
   private static final String CACHE_NAME = "test-cache";
   private static final String ERROR_MESSAGE_EXP = "ISPN(\\d)*: Lucene Directory for index '" + INDEX_NAME +
         "' can not use Cache '" + CACHE_NAME + "': store as binary enabled on the Cache configuration!";

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void failOnStoreKeysAsBinary() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable().storeKeysAsBinary(true);
      failIfStoreAsBinaryEnabled(builder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void failOnStoreValuesAsBinary() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable().storeValuesAsBinary(true);
      failIfStoreAsBinaryEnabled(builder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void failOnStoreKeysAndValuesAsBinary() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.storeAsBinary().enable()
            .storeValuesAsBinary(true)
            .storeKeysAsBinary(true);
      failIfStoreAsBinaryEnabled(builder);
   }

   private void failIfStoreAsBinaryEnabled(ConfigurationBuilder configuration) {
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
