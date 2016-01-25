package org.infinispan.persistence;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.context.Flag.SKIP_CACHE_STORE;

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
      CacheLoader cl = TestingUtil.getCacheLoader(cache);
      assert cl != null;
      MarshalledEntry se = cl.load("a");
      assert se != null;
      assert se.getValue().equals("b");

      // clear the cache
      cache.getAdvancedCache().withFlags(SKIP_CACHE_STORE).clear();

      se = cl.load("a");
      assert se != null;
      assert se.getValue().equals("b");

      // now attempt a concurrent get and evict.
      ExecutorService e = Executors.newFixedThreadPool(1);
      sdi.enabled.set(true);

      log.info("test::doing the get");

      // call the get
      Future<String> future = e.submit(new Callable<String>() {
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
      sdi.enabled.set(false);

      // and check that the key actually has been evicted
      assert !TestingUtil.extractComponent(cache, DataContainer.class).containsKey("a");

      e.shutdownNow();
   }

   public static class SlowDownInterceptor extends CommandInterceptor implements Cloneable{

      private static final long serialVersionUID = 8790944676490291484L;

      AtomicBoolean enabled = new AtomicBoolean();
      transient CountDownLatch getLatch = new CountDownLatch(1);
      transient CountDownLatch evictLatch = new CountDownLatch(1);

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (enabled.get()) {
            getLog().trace("Wait for evict to give go ahead...");
            if (!evictLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see get after 60 seconds!");
         }
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            getLog().trace("After get, now let evict go through");
            if (enabled.get()) getLatch.countDown();
         }
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         if (enabled.get()) {
            evictLatch.countDown();
            getLog().trace("Wait for get to finish...");
            if (!getLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see evict after 60 seconds!");
         }
         return invokeNextInterceptor(ctx, command);
      }
      @Override
      public SlowDownInterceptor clone(){
         try {
            return (SlowDownInterceptor) super.clone();
         } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Should not happen", e);
         }
      }
   }
}
