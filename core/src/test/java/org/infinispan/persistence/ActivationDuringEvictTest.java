package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.PassivationCacheLoaderInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests an evict command going past the entry wrapping interceptor while the entry is in the store,
 * then another thread rushing ahead and activating the entry in memory before the evict command commits.
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "persistence.ActivationDuringEvictTest")
public class ActivationDuringEvictTest extends SingleCacheManagerTest {
   public static final String KEY = "a";
   public static final String VALUE = "b";
   SlowDownInterceptor sdi;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      sdi = new SlowDownInterceptor();
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
         .persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .customInterceptors()
            .addInterceptor()
               .interceptor(sdi).after(PassivationCacheLoaderInterceptor.class)
         .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      return TestCacheManagerFactory.createCacheManager(config);
   }

   public void testActivationDuringEvict() throws Exception {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      DummyInMemoryStore cl = TestingUtil.getFirstStore(cache);

      cache.put(KEY, VALUE);
      assertEquals(VALUE, cache.get(KEY));

      MarshallableEntry se = cl.loadEntry(KEY);
      assertNull(se);

      // passivate the entry
      cache.evict(KEY);

      assertPassivated(dc, cl, KEY, VALUE);

      // start blocking evict commands
      sdi.enabled = true;

      // call the evict()
      Future<?> future = fork(() -> {
         log.info("before the evict");
         cache.evict(KEY);
         log.info("after the evict");
      });

      // wait for the SlowDownInterceptor to intercept the evict
      if (!sdi.evictLatch.await(10, TimeUnit.SECONDS))
         throw new org.infinispan.util.concurrent.TimeoutException();

      log.info("doing the get");
      Object value = cache.get(KEY);
      // make sure the get call, which would have gone past the cache loader interceptor first, gets the correct value.
      assertEquals(VALUE, value);

      sdi.getLatch.countDown();
      future.get(10, TimeUnit.SECONDS);

      // disable the SlowDownInterceptor
      sdi.enabled = false;

      // and check that the key actually has been evicted
      assertPassivated(dc, cl, KEY, VALUE);
   }

   private void assertPassivated(DataContainer dc, DummyInMemoryStore cl, String key, String expected) {
      MarshallableEntry se;
      assertFalse(dc.containsKey(key));
      se = cl.loadEntry(key);
      assertNotNull(se);
      assertEquals(expected, se.getValue());
   }

   static class SlowDownInterceptor extends DDAsyncInterceptor {
      private static final Log log = LogFactory.getLog(SlowDownInterceptor.class);

      volatile boolean enabled = false;
      CountDownLatch getLatch = new CountDownLatch(1);
      CountDownLatch evictLatch = new CountDownLatch(1);

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         if (enabled) {
            evictLatch.countDown();
            log.trace("Wait for get to finish...");
            if (!getLatch.await(10, TimeUnit.SECONDS))
               throw new TimeoutException("Didn't see evict!");
         }
         return invokeNext(ctx, command);
      }
   }
}
