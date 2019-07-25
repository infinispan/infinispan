package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.retry.SocketTimeoutFailureRetryTest")
@CleanupAfterTest
public class SocketTimeoutFailureRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
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
            //.connectionPool().maxActive(1) //this ensures that only one server is active at a time
            .addServer().host("127.0.0.1").port(port);
      return new InternalRemoteCacheManager(builder.build());
   }

   protected void addInterceptors(Cache<?, ?> cache) {
      super.addInterceptors(cache);
      cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(new DelayingInterceptor(), EntryWrappingInterceptor.class);
   }

   public void testRetrySocketTimeout() {
      Integer key = 1;
      remoteCache.put(key, "v1");
      assertEquals("v1", remoteCache.get(1));

      AdvancedCache<?, ?> nextCache = cacheToHit(key);
      DelayingInterceptor interceptor = nextCache.getAsyncInterceptorChain().findInterceptorExtending(DelayingInterceptor.class);
      interceptor.delayNextRequest();

      assertEquals(0, remoteCacheManager.getChannelFactory().getRetries());
      assertEquals("v1", remoteCache.get(key));
      assertEquals(1, remoteCacheManager.getChannelFactory().getRetries());
   }

   public static class DelayingInterceptor extends BaseCustomAsyncInterceptor {
      static AtomicBoolean delayNextRequest = new AtomicBoolean();

      public void delayNextRequest() {
         delayNextRequest.set(true);
      }

      @Override
      public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
         if (delayNextRequest.compareAndSet(true, false)) {
            return asyncValue(TestingUtil.delayed(() -> Exceptions.unchecked(() -> super.visitGetCacheEntryCommand(ctx, command)), 6_000, TimeUnit.MILLISECONDS));
         } else {
            return super.visitGetCacheEntryCommand(ctx, command);
         }
      }
   }
}
