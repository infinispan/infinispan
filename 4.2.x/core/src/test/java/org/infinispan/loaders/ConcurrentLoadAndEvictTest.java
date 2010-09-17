package org.infinispan.loaders;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.CloneableConfigurationComponent;
import org.infinispan.config.Configuration;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests a thread going past the cache loader interceptor and the interceptor deciding that loading is not necessary,
 * then another thread rushing ahead and evicting the entry from memory.
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "loaders.ConcurrentLoadAndEvictTest")
public class ConcurrentLoadAndEvictTest extends SingleCacheManagerTest {
   SlowDownInterceptor sdi;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration config = new Configuration();
      // we need a loader:
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      config.setCacheLoaderManagerConfig(clmc);
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());

      // we also need a custom interceptor to intercept get() calls after the CLI, to slow it down so an evict goes
      // through first

      sdi = new SlowDownInterceptor();
      CustomInterceptorConfig cic = new CustomInterceptorConfig(sdi);
      cic.setAfterInterceptor(CacheLoaderInterceptor.class);
      config.setCustomInterceptors(Collections.singletonList(cic));
      return TestCacheManagerFactory.createCacheManager(config);
   }
 
   public void testEvictBeforeRead() throws CacheLoaderException, ExecutionException, InterruptedException {
      cache = cacheManager.getCache();
      cache.put("a", "b");
      assert cache.get("a").equals("b");
      CacheLoader cl = TestingUtil.getCacheLoader(cache);
      assert cl != null;
      InternalCacheEntry se = cl.load("a");
      assert se != null;
      assert se.getValue().equals("b");

      // now attempt a concurrent get and evict.
      ExecutorService e = Executors.newFixedThreadPool(1);
      sdi.enabled = true;

      // call the get
      Future<String> future = e.submit(new Callable<String>() {
         public String call() throws Exception {
            return (String) cache.get("a");
         }
      });

      // now run the evict.
      cache.evict("a");

      // make sure the get call, which would have gone past the cache loader interceptor first, gets the correct value.
      assert future.get().equals("b");

      // disable the SlowDownInterceptor
      sdi.enabled = false;

      // and check that the key actually has been evicted
      assert !TestingUtil.extractComponent(cache, DataContainer.class).containsKey("a");
   }

   public static class SlowDownInterceptor extends CommandInterceptor implements CloneableConfigurationComponent{
   
      private static final long serialVersionUID = 8790944676490291484L;
   
      volatile boolean enabled = false;
      transient CountDownLatch getLatch = new CountDownLatch(1);
      transient CountDownLatch evictLatch = new CountDownLatch(1);

      @Override
      public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
         if (enabled) {
            getLatch.countDown();
            if (!evictLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see evict after 60 seconds!");
         }
         return invokeNextInterceptor(ctx, command);
      }

      @Override
      public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
         if (enabled) {
            if (!getLatch.await(60000, TimeUnit.MILLISECONDS))
               throw new TimeoutException("Didn't see get after 60 seconds!");
         }
         try {
            return invokeNextInterceptor(ctx, command);
         } finally {
            if (enabled) evictLatch.countDown();
         }
      }
      public SlowDownInterceptor clone(){
         try {
            return (SlowDownInterceptor) super.clone();
         } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Should not happen", e);
         }
      }    
   }
}
