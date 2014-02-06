package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.mockito.Mockito;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests the number of retries.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "unit", testName = "client.hotrod.retry.RetryOnFailureUnitTest")
public class RetryOnFailureUnitTest {

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
      TransportFactory mockTransport = Mockito.mock(TransportFactory.class);
      Mockito.when(mockTransport.getMaxRetries()).thenReturn(maxRetry);
      MockOperation mockOperation = new MockOperation(mockTransport, failOnTransport);
      try {
         mockOperation.execute();
         AssertJUnit.fail("Exception expected!");
      } catch (HotRodClientException expected) {
         //ignore
      }
      if (failOnTransport) {
         assertEquals("Wrong getTransport() invocation.", maxRetry + 1, mockOperation.transportInvocationCount.get());
         assertEquals("Wrong execute() invocation.", 0, mockOperation.executeInvocationCount.get());
      } else {
         assertEquals("Wrong getTransport() invocation.", maxRetry + 1, mockOperation.transportInvocationCount.get());
         assertEquals("Wrong execute() invocation.", maxRetry + 1, mockOperation.executeInvocationCount.get());
      }
   }

   private class MockOperation extends RetryOnFailureOperation<Void> {

      private final AtomicInteger transportInvocationCount;
      private final AtomicInteger executeInvocationCount;
      private final boolean failOnTransport;

      public MockOperation(TransportFactory transportFactory, boolean failOnTransport) {
         super(null, transportFactory, null, null, null);
         this.failOnTransport = failOnTransport;
         transportInvocationCount = new AtomicInteger(0);
         executeInvocationCount = new AtomicInteger(0);
      }

      @Override
      protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
         transportInvocationCount.incrementAndGet();
         if (failOnTransport) {
            throw new TransportException("Induced Failure", null);
         }
         //we can return null since it is not used
         return null;
      }

      @Override
      protected Void executeOperation(Transport transport) {
         executeInvocationCount.incrementAndGet();
         if (!failOnTransport) {
            throw new RemoteNodeSuspectException("Induced Failure", 1L, (short) 1);
         }
         //we can return null since it is not used
         return null;
      }
   }

}
