package org.infinispan.lucene.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies that the Lucene Directory correctly refuses to use a cache without fetching state from other nodes (i.e.
 * state transfer)
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lucene.configuration.NoStateTransferValidationTest")
public class NoStateTransferValidationTest extends AbstractInfinispanTest {

   private static final String INDEX_NAME = "test-index";
   private static final String CACHE_NAME = "test-cache";
   private static final String ERROR_MESSAGE_EXP = "ISPN(\\d)*: Lucene Directory for index '" + INDEX_NAME +
         "' can not use Cache '" + CACHE_NAME + "': fetch in state is not enabled in Cache configuration!";

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void testFailWithoutFetchAnyState() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      doConfigurationTest(builder);
   }

   public void testNoFailWithFetchInMemoryState() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true);
      doConfigurationTest(builder);
   }

   public void testNoFailWithFetchPersistenceState() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(true).preload(true);
      doConfigurationTest(builder);
   }

   public void testNoFailWithCacheLoaderAndFetchInMemoryState() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(false).preload(true);
      doConfigurationTest(builder);
   }

   public void testNoFailWithSharedCacheLoader() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(false).preload(true).shared(true);
      doConfigurationTest(builder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void testNoFailWithPassivationCacheLoader() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(false).preload(true);
      doConfigurationTest(builder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void testNoFailWithPassivationCacheLoader2() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(false);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(true).preload(true);
      doConfigurationTest(builder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ERROR_MESSAGE_EXP)
   public void testNoFailWithPassivationCacheLoader3() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(false).preload(true);
      doConfigurationTest(builder);
   }

   public void testNoFailWithPassivationCacheLoader4() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(true).preload(true);
      doConfigurationTest(builder);
   }

   private void doConfigurationTest(ConfigurationBuilder configuration) {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = TestCacheManagerFactory.createClusteredCacheManager(configuration);
         Cache cache = cacheManager.getCache(CACHE_NAME);

         DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).create();
      } finally {
         if (cacheManager != null) {
            TestingUtil.killCacheManagers(cacheManager);
         }
      }
   }
}
