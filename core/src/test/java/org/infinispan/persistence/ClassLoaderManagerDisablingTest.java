package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
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
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testAsyncStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).async().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testSingletonStoreDisabling() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).singleton().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      try {
         checkAndDisableStore(cm);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
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

   private void checkAndDisableStore(EmbeddedCacheManager cm) {
      checkAndDisableStore(cm, 1);
   }

   private void checkAndDisableStore(EmbeddedCacheManager cm, int count) {
      Cache<Object, Object> cache = cm.getCache();
      PersistenceManager clm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      assertEquals(count, clm.getStores(DummyInMemoryStore.class).size());
      clm.disableStore(DummyInMemoryStore.class.getName());
   }
}
