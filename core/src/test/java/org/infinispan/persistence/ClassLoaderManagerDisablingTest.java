package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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

   public void testSingletonStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).singleton().enable();
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

   private void checkAndDisableStore(EmbeddedCacheManager cm) {
      checkAndDisableStore(cm, 1);
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm, int count) {
      Cache<Object, Object> cache = cm.getCache();
      PersistenceManager clm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      assertEquals(count, clm.getStores(DummyInMemoryStore.class).size());
      clm.disableStore(DummyInMemoryStore.class.getName());
      List<CommandInterceptor> interceptors = TestingUtil.extractComponent(cache, InterceptorChain.class)
            .getInterceptorsWhichExtend(CacheLoaderInterceptor.class);
      assertTrue("Expected empty CacheLoaderInterceptor list: " + interceptors, interceptors.isEmpty());
      interceptors = TestingUtil.extractComponent(cache, InterceptorChain.class)
            .getInterceptorsWhichExtend(CacheWriterInterceptor.class);
      assertTrue("Expected empty CacheWriterInterceptor list: " + interceptors, interceptors.isEmpty());
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
      builder.eviction().strategy(EvictionStrategy.LIRS).maxEntries(1);
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
