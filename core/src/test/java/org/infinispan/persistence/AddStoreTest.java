package org.infinispan.persistence;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.ClusteredCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.PassivationCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.PassivationClusteredCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.PassivationWriterInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @since 13.0
 */
@Test(groups = "functional", testName = "persistence.AddStoreTest")
public class AddStoreTest extends AbstractInfinispanTest {

   public void testAddStore() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();

      ConfigurationBuilder toAddBuilder = new ConfigurationBuilder();
      toAddBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

      addAndCheckStore(cacheBuilder, toAddBuilder.build().persistence().stores().get(0), this::checkStore);
   }

   public void testAddAsyncStore() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();

      ConfigurationBuilder toAddBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      toAddBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).async().enable();

      addAndCheckStore(cacheBuilder, toAddBuilder.build().persistence().stores().get(0), this::checkStore);
   }

   @Test
   public void testAddStoreWithToClusteredCache() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);

      ConfigurationBuilder toAddBuilder = new ConfigurationBuilder();
      toAddBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

      addAndCheckStore(cacheBuilder, toAddBuilder.build().persistence().stores().get(0), this::checkClustered);
   }

   @Test
   public void testAddStoreWithToRrplCache() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);

      ConfigurationBuilder toAddBuilder = new ConfigurationBuilder();
      toAddBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

      addAndCheckStore(cacheBuilder, toAddBuilder.build().persistence().stores().get(0), this::checkClustered);
   }

   @Test
   public void testAddStoreWithPassivation() {
      testPassivation(new ConfigurationBuilder());
   }

   @Test
   public void testAddStoreWithPassivationToClusteredCache() {
      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      testPassivation(cacheBuilder);
   }

   @Test
   public void testAddExtraStoreToCache() {
      String location = null;
      try {
         ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
         cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
         location = CommonsTestingUtil.tmpDirectory(this.getClass());
         cacheBuilder.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class).dataLocation(location).indexLocation(location);

         ConfigurationBuilder toAddBuilder = new ConfigurationBuilder();
         toAddBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

         addAndCheckStore(cacheBuilder, toAddBuilder.build().persistence().stores().get(0), this::checkClustered);
      } finally {
         if (location != null) {
            Util.recursiveFileRemove(location);
         }
      }
   }

   @Test(expectedExceptions = CompletionException.class, expectedExceptionsMessageRegExp = ".*Cache.*is non empty.*")
   public void testAddToNonEmptyCache() {
      EmbeddedCacheManager cm = null;
      try {
         ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
         cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
         cm = createClusteredCacheManager(cacheBuilder);
         Cache<Object, Object> cache = cm.getCache();
         cache.put("k", "v");
         PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);

         ConfigurationBuilder storeBuilder = new ConfigurationBuilder();
         storeBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

         join(persistenceManager.addStore(storeBuilder.build().persistence().stores().get(0)));

      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private void addAndCheckStore(ConfigurationBuilder builder, StoreConfiguration
         storeConfiguration, Consumer<Cache<?, ?>> check) {
      EmbeddedCacheManager cm = null;
      try {
         builder.statistics().enable();
         cm = !builder.clustering().cacheMode().isClustered() ? createCacheManager(builder) : createClusteredCacheManager(builder);
         Cache<Object, Object> cache = cm.getCache();
         PersistenceManager persistenceManager = TestingUtil.extractComponent(cache, PersistenceManager.class);

         // Add store
         join(persistenceManager.addStore(storeConfiguration));

         int storesBefore = cache.getCacheConfiguration().persistence().stores().size();
         int storeSizeAfterAdding = countStores(persistenceManager);

         // Check the store was added to the persistenceManager
         assertEquals(storesBefore + 1, storeSizeAfterAdding);

         // Check the interceptors were created
         check.accept(cache);

         // Add some data
         cache.put("key1", "value1");
         cache.put("key2", "value2");

         // Check the data hit the store
         int expected = cache.getCacheConfiguration().persistence().passivation() ? 1 : 2;
         assertEquals(expected, getDummyStoreSize(persistenceManager, cache.getCacheConfiguration().clustering().hash().numSegments()));

         // Disable the store
         join(persistenceManager.disableStore(DummyInMemoryStore.class.getName()));

         // Check store is gone
         assertEquals(storesBefore, countStores(persistenceManager));

      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private int countStores(PersistenceManager pm) {
      return pm.getStoresAsString().size();
   }

   private Class loadClass(String className) {
      try {
         return Class.forName(className);
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   private long getDummyStoreSize(PersistenceManager persistenceManager, int numSegments) {
      return persistenceManager.getStoresAsString().stream()
            .filter(s -> s.equals(DummyInMemoryStore.class.getName()))
            .map(this::loadClass)
            .map(s -> persistenceManager.getStores(s).iterator().next())
            .map(store -> {
               if (store instanceof DummyInMemoryStore) {
                  DummyInMemoryStore dummyInMemoryStore = (DummyInMemoryStore) store;
                  return dummyInMemoryStore.size();
               }
               return -1L;
            }).findFirst().orElse(-1L);
   }


   private void checkStore(Cache<?, ?> cache) {
      AsyncInterceptorChain asyncInterceptorChain = extractInterceptorChain(cache);
      assertNotNull(asyncInterceptorChain.findInterceptorWithClass(CacheLoaderInterceptor.class));
      assertNotNull(asyncInterceptorChain.findInterceptorWithClass(CacheWriterInterceptor.class));
   }

   private void checkPassivation(Cache<?, ?> cache) {
      AsyncInterceptorChain asyncInterceptorChain = extractInterceptorChain(cache);
      assertNotNull(asyncInterceptorChain.findInterceptorWithClass(PassivationWriterInterceptor.class));

      if (cache.getAdvancedCache().getCacheConfiguration().clustering().cacheMode().isClustered()) {
         assertNotNull(asyncInterceptorChain.findInterceptorWithClass(PassivationClusteredCacheLoaderInterceptor.class));
      } else {
         assertNotNull(asyncInterceptorChain.findInterceptorWithClass(PassivationCacheLoaderInterceptor.class));
      }
   }

   private void checkClustered(Cache<?, ?> cache) {
      AsyncInterceptorChain asyncInterceptorChain = extractInterceptorChain(cache);
      assertNotNull(asyncInterceptorChain.findInterceptorWithClass(ClusteredCacheLoaderInterceptor.class));
   }

   private void testPassivation(ConfigurationBuilder cacheBuilder) {
      String location = null;
      try {
         location = CommonsTestingUtil.tmpDirectory(this.getClass());

         cacheBuilder.persistence().passivation(true).addStore(SoftIndexFileStoreConfigurationBuilder.class).dataLocation(location).indexLocation(location);
         cacheBuilder.memory().maxCount(1);

         ConfigurationBuilder toAddBuilder = new ConfigurationBuilder();
         toAddBuilder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);

         addAndCheckStore(cacheBuilder, toAddBuilder.build().persistence().stores().get(0), this::checkPassivation);

      } finally {
         if (location != null) {
            Util.recursiveFileRemove(location);
         }
      }
   }

}
