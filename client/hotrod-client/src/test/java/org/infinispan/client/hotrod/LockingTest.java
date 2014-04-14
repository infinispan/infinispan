package org.infinispan.client.hotrod;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.CallInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests locks over HotRod.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(testName = "client.hotrod.LockingTest", groups = "functional")
@CleanupAfterTest
public class LockingTest extends SingleCacheManagerTest {
   private RemoteCacheManager remoteCacheManager;
   private HotRodServer hotrodServer;

   public void testPerEntryLockContainer() throws Exception {
      doLockTest(CacheName.PER_ENTRY_LOCK);
   }

   public void testStrippedLockContainer() throws Exception {
      doLockTest(CacheName.STRIPPED_LOCK);
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      super.teardown();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.locking().lockAcquisitionTimeout(100, TimeUnit.MILLISECONDS);
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      for (CacheName cacheName : CacheName.values()) {
         cacheName.configure(builder);
         cacheManager.defineConfiguration(cacheName.name(), builder.build());
      }
      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost", hotrodServer.getPort());
   }

   private void doLockTest(CacheName cacheName) throws Exception {
      final RemoteCache<String, String> remoteCache = remoteCacheManager.getCache(cacheName.name());
      CheckPoint checkPoint = injectBlockingCommandInterceptor(cacheName.name());

      Future<Void> op = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            remoteCache.put("key", "value1");
            return null;
         }
      });

      checkPoint.awaitStrict("before-block", 30, TimeUnit.SECONDS);

      try {
         for (int i = 0; i < 50; ++i) {
            try {
               remoteCache.put("key", "value" + i);
               AssertJUnit.fail("It should have fail with lock timeout!");
            } catch (Exception e) {
               log.trace("Exception caught", e);
               if (!e.getLocalizedMessage().contains("Unable to acquire lock after")) {
                  //we got an unexpected exception!
                  throw e;
               }
            }
         }
      } finally {
         checkPoint.trigger("block");
      }

      op.get();

      AssertJUnit.assertEquals("value1", remoteCache.get("key"));
   }

   private CheckPoint injectBlockingCommandInterceptor(String cacheName) {
      AdvancedCache<?, ?> advancedCache = cache(cacheName).getAdvancedCache();
      final CheckPoint checkPoint = new CheckPoint();
      advancedCache.addInterceptorBefore(new BaseCustomInterceptor() {

         private final AtomicBoolean first = new AtomicBoolean(false);

         @Override
         public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
            if (first.compareAndSet(false, true)) {
               checkPoint.trigger("before-block");
               checkPoint.awaitStrict("block", 30, TimeUnit.SECONDS);
            }
            return invokeNextInterceptor(ctx, command);
         }
      }, CallInterceptor.class);
      return checkPoint;
   }

   private static enum CacheName {
      STRIPPED_LOCK {
         @Override
         void configure(ConfigurationBuilder builder) {
            builder.locking().useLockStriping(true);
         }
      },
      PER_ENTRY_LOCK {
         @Override
         void configure(ConfigurationBuilder builder) {
            builder.locking().useLockStriping(false);
         }
      };

      abstract void configure(ConfigurationBuilder builder);
   }

}
