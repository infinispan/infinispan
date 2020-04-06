package org.infinispan.persistence;

import static org.infinispan.context.Flag.SKIP_CACHE_STORE;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests a thread going past the cache loader interceptor and the interceptor deciding that loading is not necessary,
 * then another thread rushing ahead and evicting the entry from memory.
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "persistence.ConcurrentLoadAndEvictTest")
public class ConcurrentLoadAndEvictTest extends SingleCacheManagerTest {
   SlowDownInterceptor sdi;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      sdi = new SlowDownInterceptor();
      // we need a loader and a custom interceptor to intercept get() calls
      // after the CLI, to slow it down so an evict goes through first
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
         .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
         .customInterceptors()
            .addInterceptor()
               .interceptor(sdi).after(InvocationContextInterceptor.class)
         .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      return TestCacheManagerFactory.createCacheManager(config);
   }

   public void testEvictBeforeRead() throws PersistenceException, ExecutionException, InterruptedException {
      cache = cacheManager.getCache();
      cache.put("a", "b");
      assert cache.get("a").equals("b");
      DummyInMemoryStore cl = TestingUtil.getFirstStore(cache);
      MarshallableEntry se = cl.loadEntry("a");
      assert se != null;
      assert se.getValue().equals("b");

      // clear the cache
      cache.getAdvancedCache().withFlags(SKIP_CACHE_STORE).clear();

      se = cl.loadEntry("a");
      assert se != null;
      assert se.getValue().equals("b");

      // now attempt a concurrent get and evict.
      sdi.enabled = true;

      log.info("test::doing the get");

      // call the get
      Future<String> future = fork(new Callable<String>() {
         @Override
         public String call() throws Exception {
            return (String) cache.get("a");
         }
      });

      // now run the evict.
      log.info("test::before the evict");
      cache.evict("a");
      log.info("test::after the evict");

      // make sure the get call, which would have gone past the cache loader interceptor first, gets the correct value.
      assert future.get().equals("b");

      // disable the SlowDownInterceptor
      sdi.enabled = false;

      // and check that the key actually has been evicted
      assert !TestingUtil.extractComponent(cache, InternalDataContainer.class).containsKey("a");
   }

   public static class SlowDownInterceptor extends DDAsyncInterceptor {
      private static final Log log = LogFactory.getLog(SlowDownInterceptor.class);

      volatile boolean enabled = false;
      transient CountDownLatch getLatch = new CountDownLatch(1);
      transient CountDownLatch evictLatch = new CountDownLatch(1);

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (enabled) {
            log.trace("Wait for evict to give go ahead...");
            if (!evictLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see get after 60 seconds!");
         }
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
            log.trace("After get, now let evict go through");
            if (enabled) getLatch.countDown();
         });
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         if (enabled) {
            evictLatch.countDown();
            log.trace("Wait for get to finish...");
            if (!getLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see evict after 60 seconds!");
         }
         return invokeNext(ctx, command);
      }
   }
}
