package org.infinispan.functional.persistence;

import java.lang.reflect.Method;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.CacheLoaderFunctionalTest;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.persistence.FunctionalPersistenceTest")
public class FunctionalPersistenceTest extends CacheLoaderFunctionalTest {

   @Factory
   public Object[] factory() {
      return new Object[]{
            new FunctionalPersistenceTest().segmented(true),
            new FunctionalPersistenceTest().segmented(false),
      };
   }

   @Override
   protected ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence()
         .addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .storeName(this.getClass().getName()); // in order to use the same store
      return cfg;
   }

   @Override
   protected Cache<String, String> getCache(EmbeddedCacheManager cm) {
      Cache<String, String> cache = super.getCache(cm);
      return FunctionalAdvancedCache.create(cache.getAdvancedCache());
   }

   @Override
   protected Cache<String, String> getCache(EmbeddedCacheManager cm, String name) {
      Cache<String, String> cache = super.getCache(cm, name);
      return FunctionalAdvancedCache.create(cache.getAdvancedCache());
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testDuplicatePersistence(Method m) throws Exception {
      super.testDuplicatePersistence(m);
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testNullFoundButLoaderReceivedValueLaterInTransaction() throws SystemException, NotSupportedException {
      super.testNullFoundButLoaderReceivedValueLaterInTransaction();
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testPreloading() throws Exception {
      super.testPreloading();
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testPreloadingWithEviction() throws Exception {
      super.testPreloadingWithEviction();
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testPreloadingWithEvictionAndOneMaxEntry() throws Exception {
      super.testPreloadingWithEvictionAndOneMaxEntry();
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testPreloadingWithoutAutoCommit() throws Exception {
      super.testPreloadingWithoutAutoCommit();
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testTransactionalWrites() throws Exception {
      super.testTransactionalWrites();
   }

   @Override @Test(enabled = false, description = "Transactional support not yet in place")
   public void testTransactionalReplace(Method m) throws Exception {
      super.testTransactionalReplace(m);
   }



}
