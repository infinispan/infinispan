package org.infinispan.client.hotrod.retry;

import static org.infinispan.client.hotrod.impl.Util.await;
import static org.testng.AssertJUnit.assertEquals;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.AbstractInfinispanTest;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Tests the number of retries.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "unit", testName = "client.hotrod.retry.RetryOnFailureUnitTest")
public class RetryOnFailureUnitTest extends AbstractInfinispanTest {

   private final Channel mockChannel = Mockito.mock(Channel.class);

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
      TestOperation testOperation = new TestOperation(mockTransport, failOnTransport);

      Exceptions.expectExceptionNonStrict(HotRodClientException.class, () -> await(testOperation.execute(), 10000));

      if (failOnTransport) {
         // Number of retries doubles as a result of dealing with complete shutdown recoveries
         assertEquals("Wrong getChannel() invocation.", maxRetry + 1, testOperation.channelInvocationCount.get());
         assertEquals("Wrong execute() invocation.", 0, testOperation.executeInvocationCount.get());
      } else {
         assertEquals("Wrong getChannel() invocation.", maxRetry + 1, testOperation.channelInvocationCount.get());
         assertEquals("Wrong execute() invocation.", maxRetry + 1, testOperation.executeInvocationCount.get());
      }
   }

   private class TestOperation extends RetryOnFailureOperation<Void> {

      private final AtomicInteger channelInvocationCount;
      private final AtomicInteger executeInvocationCount;
      private final boolean failOnTransport;

      TestOperation(ChannelFactory channelFactory, boolean failOnTransport) {
         super(ILLEGAL_OP_CODE, ILLEGAL_OP_CODE, null, channelFactory, null, null, 0,
               HotRodClientTestingUtil.newRemoteConfigurationBuilder().build(), null, null);
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
      public void writeBytes(Channel channel, ByteBuf buf) {
         throw new UnsupportedOperationException("TODO!");
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
      public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
         throw new UnsupportedOperationException();
      }
   }

}
