package org.infinispan.client.hotrod.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.test.NoopChannelOperation;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.channel.Channel;

@Test(groups = "functional", testName = "client.hotrod.retry.SingleServerSocketTimeoutTest")
public class SingleServerSocketTimeoutTest extends SocketTimeoutFailureRetryTest {

   {
      // This reproduces the case when an operation times out but there is only a single server.
      // The operation is then registered again on the same channel and it succeeds.
      nbrOfServers = 1;
   }

   public void testChannelIsReUtilizedForRetry() throws Exception {
      Integer key = 2;
      remoteCache.put(key, "v1");
      assertEquals("v1", remoteCache.get(key));

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

      Channel initialChannel = remoteCacheManager.getChannelFactory()
            .fetchChannelAndInvoke(getAddress(hotRodServer1), new NoopChannelOperation()).get(10, TimeUnit.SECONDS);

      // We keep our reference but return it to the pool.
      ChannelRecord.of(initialChannel).release(initialChannel);

      assertThat(initialChannel.isActive()).isTrue();
      delay.complete(null);

      assertEquals("v1", remoteCache.get(key));
      assertEquals(1, remoteCacheManager.getChannelFactory().getRetries());

      Channel other = remoteCacheManager.getChannelFactory()
            .fetchChannelAndInvoke(getAddress(hotRodServer1), new NoopChannelOperation()).get(10, TimeUnit.SECONDS);

      // We still have the same number of connections.
      assertEquals(connectionsBefore, channelFactory.getNumActive() + channelFactory.getNumIdle());
      assertThat(initialChannel.isActive()).isTrue();
      assertThat(other).isSameAs(initialChannel);
   }
}
