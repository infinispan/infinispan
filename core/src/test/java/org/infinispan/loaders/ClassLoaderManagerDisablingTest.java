package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.manager.CacheLoaderManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * ClassLoaderManagerDisablingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(groups="functional", testName="loaders.ClassLoaderManagerDisablingTest")
public class ClassLoaderManagerDisablingTest extends AbstractInfinispanTest {

   public void testStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testAsyncStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).async().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testSingletonStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).singletonStore().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testChainingStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).loaders().addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).async().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm, 2);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm) {
      checkAndDisableStore(cm, 1);
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm, int count) {
      Cache<Object, Object> cache = cm.getCache();
      CacheLoaderManager clm = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      assertTrue(clm.isEnabled());
      assertEquals(count, clm.getCacheLoaders(DummyInMemoryCacheStore.class).size());
      clm.disableCacheStore(DummyInMemoryCacheStore.class.getName());
      assertFalse(clm.isEnabled());
   }
}
