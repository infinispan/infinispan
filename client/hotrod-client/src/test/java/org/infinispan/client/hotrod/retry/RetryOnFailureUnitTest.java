package org.infinispan.client.hotrod.retry;

import static org.infinispan.client.hotrod.impl.Util.await;
import static org.testng.AssertJUnit.assertEquals;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory.ClusterSwitchStatus;
import org.infinispan.test.Exceptions;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.EventExecutor;

/**
 * Tests the number of retries.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "unit", testName = "client.hotrod.retry.RetryOnFailureUnitTest")
public class RetryOnFailureUnitTest {

   private EventExecutor mockExecutor = Mockito.mock(EventExecutor.class, invocation -> {
      throw new UnsupportedOperationException(invocation.toString());
   });
   private Channel mockChannel = Mockito.mock(Channel.class);

   public void testNoRetryOnTransportFailure() {
      doRetryTest(0, true);
   }

   public void testNoRetryOnExecuteFailure() {
      doRetryTest(0, false);
   }

   public void testSingleRetryOnTransportFailure() {
      doRetryTest(1, true);
   }

   public void testSingleRetryOnExecuteFailure() {
      doRetryTest(1, false);
   }

   public void testMultipleRetryOnTransportFailure() {
      doRetryTest(ConfigurationProperties.DEFAULT_MAX_RETRIES, true);
   }

   public void testMultipleRetryOnExecuteFailure() {
      doRetryTest(ConfigurationProperties.DEFAULT_MAX_RETRIES, false);
   }

   private void doRetryTest(int maxRetry, boolean failOnTransport) {
      ChannelFactory mockTransport = Mockito.mock(ChannelFactory.class);
      Mockito.when(mockTransport.getMaxRetries()).thenReturn(maxRetry);
      Mockito.when(mockTransport.trySwitchCluster(Mockito.any(), Mockito.any()))
            .thenReturn(CompletableFuture.completedFuture(ClusterSwitchStatus.NOT_SWITCHED));
      MockOperation mockOperation = new MockOperation(mockTransport, failOnTransport);
      Mockito.doReturn(true).when(mockExecutor).inEventLoop();
      // Netty executes only up to 8 listeners in single stack and we have more retries
      Mockito.doAnswer(invocation -> {
         ((Runnable) invocation.getArgument(0)).run();
         return null;
      }).when(mockExecutor).execute(Mockito.any(Runnable.class));
      Mockito.doReturn(true).when(mockChannel).isActive();

      Exceptions.expectExceptionNonStrict(HotRodClientException.class, () -> await(mockOperation.execute(), 10000));

      if (failOnTransport) {
         // Number of retries doubles as a result of dealing with complete shutdown recoveries
         assertEquals("Wrong getChannel() invocation.", maxRetry + 1, mockOperation.channelInvocationCount.get());
         assertEquals("Wrong execute() invocation.", 0, mockOperation.executeInvocationCount.get());
      } else {
         assertEquals("Wrong getChannel() invocation.", maxRetry + 1, mockOperation.channelInvocationCount.get());
         assertEquals("Wrong execute() invocation.", maxRetry + 1, mockOperation.executeInvocationCount.get());
      }
   }

   private class MockOperation extends RetryOnFailureOperation<Void> {

      private final AtomicInteger channelInvocationCount;
      private final AtomicInteger executeInvocationCount;
      private final boolean failOnTransport;

      MockOperation(ChannelFactory channelFactory, boolean failOnTransport) {
         super(null, channelFactory, null, null, 0, new ConfigurationBuilder().build());
         this.failOnTransport = failOnTransport;
         channelInvocationCount = new AtomicInteger(0);
         executeInvocationCount = new AtomicInteger(0);
      }

      @Override
      protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
         channelInvocationCount.incrementAndGet();
         if (failOnTransport) {
            cancel(null, new RemoteNodeSuspectException("Induced Failure", 1L, (short) 1));
         } else {
            invoke(mockChannel);
         }
      }

      @Override
      protected void executeOperation(Channel channel) {
         executeInvocationCount.incrementAndGet();
         if (!failOnTransport) {
            exceptionCaught(null, new RemoteNodeSuspectException("Induced Failure", 1L, (short) 1));
         } else {
            //we can return null since it is not used
            complete(null);
         }
      }

      @Override
      public Void decodePayload(ByteBuf buf, short status) {
         throw new UnsupportedOperationException();
      }
   }

}
