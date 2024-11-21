package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.SocketTimeoutFailureRetryTest")
@CleanupAfterTest
public class SocketTimeoutFailureRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      return builder;
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager(int port) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder
            .connectionTimeout(2_000)
            .socketTimeout(2_000)
            .maxRetries(1)
            .connectionPool().maxActive(1) //this ensures that only one server is active at a time
            .addServer().host("127.0.0.1").port(port);
      return new InternalRemoteCacheManager(builder.build());
   }

   protected void addInterceptors(Cache<?, ?> cache) {
      super.addInterceptors(cache);
      TestingUtil.extractInterceptorChain(cache)
                 .addInterceptorAfter(new DelayingInterceptor(), EntryWrappingInterceptor.class);
   }

   public void testRetrySocketTimeout() {
      Integer key = 1;
      remoteCache.put(key, "v1");
      assertEquals("v1", remoteCache.get(1));

      AdvancedCache<?, ?> nextCache = cacheToHit(key);
      DelayingInterceptor interceptor = TestingUtil.extractInterceptorChain(nextCache)
                                                   .findInterceptorExtending(DelayingInterceptor.class);
      CompletableFuture<Void> delay = new CompletableFuture<>();
      interceptor.delayNextRequest(delay);

      assertEquals(0, remoteCacheManager.getChannelFactory().getRetries());
      int connectionsBefore = channelFactory.getNumActive() + channelFactory.getNumIdle();
      assertEquals("v1", remoteCache.get(key));
      assertEquals(1, remoteCacheManager.getChannelFactory().getRetries());
      assertEquals(connectionsBefore, channelFactory.getNumActive() + channelFactory.getNumIdle());

      delay.complete(null);
   }

   public static class DelayingInterceptor extends BaseCustomAsyncInterceptor {
      static volatile AtomicReference<CompletionStage<Void>> delayNextRequest = new AtomicReference<>();

      public void delayNextRequest(CompletionStage<Void> delayStage) {
         delayNextRequest.set(delayStage);
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) {
         // Delay just one invocation, then reset to null
         CompletionStage<Void> delay = delayNextRequest.getAndSet(null);
         return asyncInvokeNext(ctx, command, delay);
      }
   }
}
