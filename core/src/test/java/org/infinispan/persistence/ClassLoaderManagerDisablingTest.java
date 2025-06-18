package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * ClassLoaderManagerDisablingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups = "functional", testName = "persistence.ClassLoaderManagerDisablingTest")
public class ClassLoaderManagerDisablingTest extends AbstractInfinispanTest {

   public void testStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      disableWithConfiguration(builder);
   }

   public void testAsyncStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).async().enable();
      disableWithConfiguration(builder);
   }

   public void testChainingStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).async().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm, 2);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testDisablingWithPassivation() {
      //test: PassivationInterceptor/ActivationInterceptor
      ConfigurationBuilder builder = createPersistenceConfiguration();
      enablePassivation(builder);
      disableWithConfiguration(builder);
   }

   public void testDisablingWithClusteredPassivation() {
      //test: PassivationInterceptor/ClustererActivationInterceptor
      ConfigurationBuilder builder = createClusterConfiguration(CacheMode.DIST_SYNC);
      enablePassivation(builder);
      disableWithClusteredConfiguration(builder);
   }

   public void testClusteredDisabling() {
      //test: ClusteredCacheLoaderInterceptor/DistCacheWriterInterceptor
      ConfigurationBuilder builder = createClusterConfiguration(CacheMode.DIST_SYNC);
      disableWithClusteredConfiguration(builder);
   }

   public void testDisableWithMultipleStores() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      PersistenceConfigurationBuilder p = builder.persistence();
      p.addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(true);
      // Just add this one as it is simple and we need different types
      p.addStore(UnnecessaryLoadingTest.CountingStoreConfigurationBuilder.class);

      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = TestCacheManagerFactory.createCacheManager(builder);
         Cache<Object, Object> cache = cacheManager.getCache();
         PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
         // Get all types of stores
         Set<Object> stores = pm.getStores(Object.class);
         // Should have 2 before we disable
         assertEquals(2, stores.size());

         pm.disableStore(UnnecessaryLoadingTest.CountingStore.class.getName());

         stores = pm.getStores(Object.class);
         assertEquals(1, stores.size());

         DummyInMemoryStore store = (DummyInMemoryStore) stores.iterator().next();
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm) {
      checkAndDisableStore(cm, 1);
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm, int count) {
      Cache<Object, Object> cache = cm.getCache();
      PersistenceManager clm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      Set<DummyInMemoryStore> stores = clm.getStores(DummyInMemoryStore.class);
      assertEquals(count, stores.size());
      stores.forEach(store -> assertTrue(store.isRunning()));

      clm.disableStore(DummyInMemoryStore.class.getName());
      stores.forEach(store -> assertFalse(store.isRunning()));
      AsyncInterceptor interceptor = extractInterceptorChain(cache)
            .findInterceptorExtending(CacheLoaderInterceptor.class);
      assertNull(interceptor);
      interceptor = extractInterceptorChain(cache)
            .findInterceptorExtending(CacheWriterInterceptor.class);
      assertNull(interceptor);
   }

   private ConfigurationBuilder createClusterConfiguration(CacheMode cacheMode) {
      ConfigurationBuilder builder = createPersistenceConfiguration();
      builder.clustering().cacheMode(cacheMode);
      return builder;
   }

   private ConfigurationBuilder createPersistenceConfiguration() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return builder;
   }

   private void enablePassivation(ConfigurationBuilder builder) {
      builder.persistence().passivation(true);
      builder.memory().maxCount(1);
   }

   private void disableWithConfiguration(ConfigurationBuilder builder) {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = TestCacheManagerFactory.createCacheManager(builder);
         checkAndDisableStore(cacheManager);
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   private void disableWithClusteredConfiguration(ConfigurationBuilder builder) {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = TestCacheManagerFactory.createClusteredCacheManager(builder);
         checkAndDisableStore(cacheManager);
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }
}
