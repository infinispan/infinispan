package org.infinispan.spring.remote.provider;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.cache.Cache;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringRemoteCacheManagerWithReadWriteTimeoutTest}.
 * </p>
 *
 * @author Tristan Tarrant
 */
@Test(testName = "spring.provider.SpringRemoteCacheManagerWithReadWriteTimeoutTest", groups = {"functional", "smoke"})
public class SpringRemoteCacheManagerWithReadWriteTimeoutTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "spring.remote.cache.manager.Test";

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(TEST_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      return cacheManager;
   }

   @BeforeClass
   public void setupRemoteCacheFactory() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager, ServerTestingUtil.findFreePort(), serverBuilder);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(builder.build());
   }

   @AfterClass
   public void destroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   /**
    * Test method for {@link SpringRemoteCacheManager#getCache(String)}.
    */
   @Test
   public final void springRemoteCacheManagerWithTimeoutShouldThrowTimeoutExceptions() {
      final SpringRemoteCacheManager objectUnderTest = new SpringRemoteCacheManager(
            remoteCacheManager, 500, 750);

      final Cache defaultCache = objectUnderTest.getCache(TEST_CACHE_NAME);

      assertEquals("getCache(" + TEST_CACHE_NAME + ") should have returned a cache name \""
                  + TEST_CACHE_NAME + "\". However, the returned cache has a different name.",
            TEST_CACHE_NAME, defaultCache.getName());

      defaultCache.put("k1", "v1");
      CountDownLatch latch = new CountDownLatch(1);
      extractInterceptorChain(cacheManager.getCache(TEST_CACHE_NAME))
            .addInterceptor(new DelayingInterceptor(latch, 700, 800), 0);
      Exceptions.expectException(CacheException.class, TimeoutException.class, () -> defaultCache.get("k1"));

      Exceptions.expectException(CacheException.class, TimeoutException.class, () -> defaultCache.put("k1", "v2"));
   }

   static class DelayingInterceptor extends DDAsyncInterceptor {
      private final long readDelay;
      private final long writeDelay;
      private final CountDownLatch latch;

      private DelayingInterceptor(CountDownLatch latch, long readDelay, long writeDelay) {
         this.latch = latch;
         this.readDelay = readDelay;
         this.writeDelay = writeDelay;
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         latch.await(readDelay, TimeUnit.MILLISECONDS);
         return super.visitGetCacheEntryCommand(ctx, command);
      }

      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         latch.await(writeDelay, TimeUnit.MILLISECONDS);
         return super.visitPutKeyValueCommand(ctx, command);
      }

   }
}
