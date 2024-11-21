package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.test.AbstractInfinispanTest;
import org.mockito.Mockito;
import org.testng.annotations.Test;

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
      // TODO: need to rewrite this test
//      ChannelFactory mockTransport = Mockito.mock(ChannelFactory.class);
//      Mockito.when(mockTransport.getMaxRetries()).thenReturn(maxRetry);
//      TestOperation testOperation = new TestOperation(mockTransport, failOnTransport);
//
//      Exceptions.expectExceptionNonStrict(HotRodClientException.class, () -> await(testOperation.execute(), 10000));
//
//      if (failOnTransport) {
//         // Number of retries doubles as a result of dealing with complete shutdown recoveries
//         assertEquals("Wrong getChannel() invocation.", maxRetry + 1, testOperation.channelInvocationCount.get());
//         assertEquals("Wrong execute() invocation.", 0, testOperation.executeInvocationCount.get());
//      } else {
//         assertEquals("Wrong getChannel() invocation.", maxRetry + 1, testOperation.channelInvocationCount.get());
//         assertEquals("Wrong execute() invocation.", maxRetry + 1, testOperation.executeInvocationCount.get());
//      }
   }

}
