/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;
import static org.infinispan.transaction.TransactionMode.TRANSACTIONAL;
import static org.testng.AssertJUnit.*;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "loaders.PreloadWithAsyncStoreTest")
public class PreloadWithAsyncStoreTest extends SingleCacheManagerTest {

   private static final Object[] KEYS = new Object[]{"key_1", "key_2", "key_3", "key_4"};
   private static final Object[] VALUES = new Object[]{"value_1", "value_2", "value_3", "value_4"};

   public void testtPreloadWithNonTransactionalCache() throws Exception {
      doTest(CacheType.NO_TRANSACTIONAL);
   }

   public void testtPreloadWithTransactionalUsingSynchronizationCache() throws Exception {
      doTest(CacheType.TRANSACTIONAL_SYNCHRONIZATION);
   }

   public void testPreloadWithTransactionalUsingXACache() throws Exception {
      doTest(CacheType.TRANSACTIONAL_XA);
   }

   public void testPreloadWithTransactionalUsingXAAndRecoveryCache() throws Exception {
      doTest(CacheType.TRANSACTIONAL_XA_RECOVERY);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();

      for (CacheType cacheType : CacheType.values()) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.loaders().preload(true)
               .addLoader(DummyInMemoryCacheStoreConfigurationBuilder.class)
               .storeName(this.getClass().getName()).async().enable();
         builder.transaction().transactionMode(cacheType.transactionMode).useSynchronization(cacheType.useSynchronization)
               .recovery().enabled(cacheType.useRecovery);
         builder.customInterceptors().addInterceptor().index(0).interceptor(new ExceptionTrackerInterceptor());
         cm.defineConfiguration(cacheType.cacheName, builder.build());

      }

      return cm;
   }

   protected void doTest(CacheType cacheType) throws Exception {
      final Cache<Object, Object> cache = cacheManager.getCache(cacheType.cacheName);
      ExceptionTrackerInterceptor interceptor = getInterceptor(cache);
      assertTrue("Preload should be enabled.", cache.getCacheConfiguration().loaders().preload());
      assertTrue("Async Store should be enabled.", cache.getCacheConfiguration().loaders().usingAsyncStore());

      CacheStore store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();

      assertNotInCacheAndStore(cache, store, KEYS);

      for (int i = 0; i < KEYS.length; ++i) {
         cache.put(KEYS[i], VALUES[i]);
      }

      for (int i = 1; i < KEYS.length; i++) {
         assertInCacheAndStore(cache, store, KEYS[i], VALUES[i]);
      }

      DataContainer dataContainer = cache.getAdvancedCache().getDataContainer();

      assertEquals("Wrong number of keys in data container after puts.", KEYS.length, dataContainer.size());
      assertEquals("Some exceptions has been caught during the puts.", 0, interceptor.exceptionsCaught);
      cache.stop();
      assertEquals("Expected empty data container after stop.", 0, dataContainer.size());
      assertEquals("Some exceptions has been caught during the stop.", 0, interceptor.exceptionsCaught);

      cache.start();
      assertTrue("Preload should be enabled after restart.", cache.getCacheConfiguration().loaders().preload());
      assertTrue("Async Store should be enabled after restart.", cache.getCacheConfiguration().loaders().usingAsyncStore());

      dataContainer = cache.getAdvancedCache().getDataContainer();
      assertEquals("Wrong number of keys in data container after preload.", KEYS.length, dataContainer.size());
      assertEquals("Some exceptions has been caught during the preload.", 0, interceptor.exceptionsCaught);

      // Re-retrieve since the old reference might not be usable
      store = TestingUtil.extractComponent(cache, CacheLoaderManager.class).getCacheStore();
      for (int i = 1; i < KEYS.length; i++) {
         assertInCacheAndStore(cache, store, KEYS[i], VALUES[i]);
      }
   }

   private void assertInCacheAndStore(Cache cache, CacheStore store, Object key, Object value) throws CacheLoaderException {
      InternalCacheEntry se = cache.getAdvancedCache().getDataContainer().get(key);
      assertStoredEntry(se, value, "Cache", key);
      se = store.load(key);
      assertStoredEntry(se, value, "Store", key);
   }

   private void assertStoredEntry(InternalCacheEntry entry, Object expectedValue, String src, Object key) {
      assertNotNull(src + " entry for key " + key + " should NOT be null", entry);
      assertEquals(src + " should contain value " + expectedValue + " under key " + entry.getKey() + " but was " +
                         entry.getValue() + ". Entry is " + entry, expectedValue, entry.getValue());
   }

   private <T> void assertNotInCacheAndStore(Cache cache, CacheStore store, T... keys) throws CacheLoaderException {
      for (Object key : keys) {
         assertFalse("Cache should not contain key " + key, cache.getAdvancedCache().getDataContainer().containsKey(key));
         assertFalse("Store should not contain key " + key, store.containsKey(key));
      }
   }

   private ExceptionTrackerInterceptor getInterceptor(Cache cache) {
      return (ExceptionTrackerInterceptor) TestingUtil.extractComponent(cache, InterceptorChain.class)
            .getInterceptorsWithClass(ExceptionTrackerInterceptor.class)
            .get(0);
   }

   private static enum CacheType {
      NO_TRANSACTIONAL("NO_TX"),
      TRANSACTIONAL_SYNCHRONIZATION(TRANSACTIONAL, "TX_SYNC", true, false),
      TRANSACTIONAL_XA(TRANSACTIONAL, "TX_XA", false, false),
      TRANSACTIONAL_XA_RECOVERY(TRANSACTIONAL, "TX_XA_RECOVERY", false, true);
      final TransactionMode transactionMode;
      final String cacheName;
      final boolean useSynchronization;
      final boolean useRecovery;

      private CacheType(TransactionMode transactionMode, String cacheName, boolean useSynchronization, boolean useRecovery) {
         this.transactionMode = transactionMode;
         this.cacheName = cacheName;
         this.useSynchronization = useSynchronization;
         this.useRecovery = useRecovery;
      }

      private CacheType(String cacheName) {
         //no tx cache. the boolean parameters are ignored.
         this(NON_TRANSACTIONAL, cacheName, false, false);
      }
   }

   private class ExceptionTrackerInterceptor extends CommandInterceptor {

      private volatile int exceptionsCaught = 0;

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } catch (Throwable throwable) {
            exceptionsCaught++;
            throw throwable;
         }
      }
   }

}
