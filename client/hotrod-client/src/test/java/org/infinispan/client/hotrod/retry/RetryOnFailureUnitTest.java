package org.infinispan.client.hotrod.retry;

import static org.testng.AssertJUnit.assertEquals;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.operations.RetryOnFailureOperation;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests the number of retries.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "unit", testName = "client.hotrod.retry.RetryOnFailureUnitTest")
public class RetryOnFailureUnitTest {
   private ThreadPoolExecutor asyncExecutor;

   @BeforeClass
   public void beforeClass() {
      asyncExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
   }

   @AfterClass
   public void afterClass() {
      asyncExecutor.shutdown();
   }

   public void testNoRetryOnTransportFailure() {
      doRetryTest(0, true, false, false);
   }

   public void testNoRetryOnWriteRequestFailure() {
      doRetryTest(0, false, true, false);
   }

   public void testNoRetryOnReadResponseFailure() {
      doRetryTest(0, false, false, true);
   }

   public void testSingleRetryOnTransportFailure() {
      doRetryTest(1, true, false, false);
   }

   public void testSingleRetryOnWriteRequestFailure() {
      doRetryTest(1, false, true, false);
   }

   public void testSingleRetryOnReadResponseFailure() {
      doRetryTest(1, false, false, true);
   }

   public void testMultipleRetryOnTransportFailure() {
      doRetryTest(ConfigurationProperties.DEFAULT_MAX_RETRIES, true, false, false);
   }

   public void testMultipleRetryOnWriteRequestFailure() {
      doRetryTest(ConfigurationProperties.DEFAULT_MAX_RETRIES, false, true, false);
   }

   public void testMultipleRetryOnReadResponseFailure() {
      doRetryTest(ConfigurationProperties.DEFAULT_MAX_RETRIES, false, false, true);
   }

   private void doRetryTest(int maxRetry, boolean failOnTransport, boolean failOnWrite, boolean failOnRead) {
      Transport mockTransport = Mockito.mock(Transport.class);
      Mockito.when(mockTransport.flush(Mockito.<Callable<Void>>any())).thenAnswer(new Answer<NotifyingFuture<Void>>() {
         @Override
         public NotifyingFuture<Void> answer(InvocationOnMock invocation) throws Throwable {
            final Callable<Void> callable = (Callable<Void>) invocation.getArguments()[0];
            final NotifyingFutureImpl<Void> nf = new NotifyingFutureImpl<Void>();
            FutureTask<Void> future = new FutureTask(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  try {
                     callable.call();
                     nf.notifyDone(null);
                     return null;
                  } catch (Exception e) {
                     nf.notifyException(e);
                     throw e;
                  }
               }
            });
            nf.setFuture(future);
            asyncExecutor.execute(future);
            return nf;
         }
      });

      TransportFactory mockTransportFactory = Mockito.mock(TransportFactory.class);
      Mockito.when(mockTransportFactory.getMaxRetries()).thenReturn(maxRetry);
      Mockito.when(mockTransportFactory.getTransport(Mockito.anySet())).thenReturn(mockTransport);

      MockOperation mockOperation = new MockOperation(mockTransportFactory, failOnTransport, failOnWrite, failOnRead);
      try {
         mockOperation.executeSync();
         AssertJUnit.fail("Exception expected!");
      } catch (HotRodClientException expected) {
         //ignore
      }
      if (failOnTransport) {
         assertEquals("Wrong getTransport() invocation.", maxRetry + 1, mockOperation.getTransportInvocationCount.get());
         assertEquals("Wrong writeRequest() invocation.", 0, mockOperation.writeRequestInvocationCount.get());
         assertEquals("Wrong readResponse() invocation.", 0, mockOperation.readResponseInvocationCount.get());
      } else if (failOnWrite) {
         assertEquals("Wrong getTransport() invocation.", maxRetry + 1, mockOperation.getTransportInvocationCount.get());
         assertEquals("Wrong writeRequest() invocation.", maxRetry + 1, mockOperation.writeRequestInvocationCount.get());
         assertEquals("Wrong readResponse() invocation.", 0, mockOperation.readResponseInvocationCount.get());
      } else {
         assertEquals("Wrong getTransport() invocation.", maxRetry + 1, mockOperation.getTransportInvocationCount.get());
         assertEquals("Wrong writeRequest() invocation.", maxRetry + 1, mockOperation.writeRequestInvocationCount.get());
         assertEquals("Wrong readResponse() invocation.", maxRetry + 1, mockOperation.readResponseInvocationCount.get());
      }
      assertEquals("Wrong get/release transport invocation.",
            mockOperation.getTransportInvocationCount.get(), mockOperation.releaseTransportInvocationCount.get());
   }

   private class MockOperation extends RetryOnFailureOperation<Void> {

      private final AtomicInteger getTransportInvocationCount;
      private final AtomicInteger releaseTransportInvocationCount;
      private final AtomicInteger writeRequestInvocationCount;
      private final AtomicInteger readResponseInvocationCount;
      private final boolean failOnTransport;
      private final boolean failOnWrite;
      private final boolean failOnRead;

      public MockOperation(TransportFactory transportFactory, boolean failOnTransport, boolean failOnWrite, boolean failOnRead) {
         super(null, transportFactory, null, null, null);
         this.failOnTransport = failOnTransport;
         this.failOnWrite = failOnWrite;
         this.failOnRead = failOnRead;
         getTransportInvocationCount = new AtomicInteger(0);
         releaseTransportInvocationCount = new AtomicInteger(0);
         writeRequestInvocationCount = new AtomicInteger(0);
         readResponseInvocationCount = new AtomicInteger(0);
      }

      @Override
      protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
         getTransportInvocationCount.incrementAndGet();
         if (failOnTransport) {
            throw new TransportException("Induced Failure", null);
         }
         //we can return null since it is not used
         return transportFactory.getTransport(failedServers);
      }

      @Override
      protected void releaseTransport(Transport transport) {
         releaseTransportInvocationCount.incrementAndGet();
         super.releaseTransport(transport);
      }

      @Override
      protected HeaderParams writeRequest(Transport transport) {
         writeRequestInvocationCount.incrementAndGet();
         if (failOnWrite) {
            throw new RemoteNodeSuspectException("Induced Failure", 1L, (short) 1);
         }
         return new HeaderParams();
      }

      @Override
      protected Void readResponse(Transport transport, HeaderParams params) {
         readResponseInvocationCount.incrementAndGet();
         if (failOnRead) {
            throw new RemoteNodeSuspectException("Induced Failure", 1L, (short) 1);
         }
         return null;
      }
   }

}
