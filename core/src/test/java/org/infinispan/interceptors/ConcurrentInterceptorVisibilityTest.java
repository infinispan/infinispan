package org.infinispan.interceptors;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests visibility of effects of cache operations on a separate thread once
 * they've passed a particular interceptor barrier related to the cache
 * operation.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "interceptors.ConcurrentInterceptorVisibilityTest")
public class ConcurrentInterceptorVisibilityTest extends AbstractInfinispanTest {

   public void testSizeVisibility() throws Exception {
      updateCache(Visibility.SIZE);
   }

   @Test(groups = "unstable")
   public void testGetVisibility() throws Exception {
      updateCache(Visibility.GET);
   }

   private void updateCache(final Visibility visibility) {
      final String key = "k-" + visibility;
      final String value = "k-" + visibility;
      final CountDownLatch entryCreatedLatch = new CountDownLatch(1);
      final EntryCreatedInterceptor interceptor = new EntryCreatedInterceptor(entryCreatedLatch);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.addInterceptor(global, TestCacheManagerFactory.DEFAULT_CACHE_NAME::equals, interceptor, TestCacheManagerFactory.InterceptorPosition.BEFORE, EntryWrappingInterceptor.class);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(global, builder)) {
         @Override
         public void call() throws Exception {
            final Cache<Object,Object> cache = cm.getCache();

            switch (visibility) {
               case SIZE:
                  assertTrue(cache.isEmpty());
                  break;
               case GET:
                  assertNull(cache.get(key));
                  break;
            }

            Future<Void> ignore = fork(() -> {
               cache.put(key, value);
               return null;
            });

            try {
               entryCreatedLatch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }

            switch (visibility) {
               case SIZE:
                  int size = cache.size();
                  assertTrue(size == 1, "size is: " + size);
                  assertTrue(interceptor.assertKeySet);
                  break;
               case GET:
                  Object retVal = cache.get(key);
                  assertNotNull(retVal);
                  assertEquals(value, retVal, "retVal is: " + retVal);
                  assertTrue(interceptor.assertKeySet);
                  break;
            }

            ignore.get(5, TimeUnit.SECONDS);
         }
      });
   }

   private enum Visibility {
      SIZE, GET
   }

   public static class EntryCreatedInterceptor extends BaseCustomAsyncInterceptor {
      private static final Log log = LogFactory.getLog(EntryCreatedInterceptor.class);

      final CountDownLatch latch;
      volatile boolean assertKeySet;

      private EntryCreatedInterceptor(CountDownLatch latch) {
         this.latch = latch;
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx,
               PutKeyValueCommand command) throws Throwable {
         // First execute the operation itself
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            assertKeySet = (cache.keySet().size() == 1);
            // After entry has been committed to the container
            log.info("Cache entry created, now check in different thread");
            latch.countDown();
            // Force a bit of delay in the listener
            TestingUtil.sleepThread(3000);
         });
      }

   }

}
