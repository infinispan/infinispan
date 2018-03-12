package org.infinispan.lock.impl.lock;

import static org.infinispan.test.Exceptions.expectException;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "clusteredLock.RequestExpirationSchedulerTest")
public class RequestExpirationSchedulerTest {

   private RequestExpirationScheduler expirationScheduler;
   private ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
   private ScheduledFuture scheduledFutureMock;
   private CompletableFuture<Boolean> booleanCompletableFuture;

   @BeforeMethod
   public void createRequestExpirationScheduler() {
      expirationScheduler = new RequestExpirationScheduler(scheduledExecutorService);
      scheduledFutureMock = mock(ScheduledFuture.class);
      booleanCompletableFuture = new CompletableFuture();
      when(scheduledExecutorService.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(scheduledFutureMock);
   }

   @Test
   public void testScheduleForCompletionAndRunScheduling() throws Exception {

      ArgumentCaptor<Runnable> argument = ArgumentCaptor.forClass(Runnable.class);

      expirationScheduler.scheduleForCompletion("123", booleanCompletableFuture, 10, TimeUnit.MILLISECONDS);
      assertEquals(1, expirationScheduler.countScheduledRequests());

      verify(scheduledExecutorService).schedule(argument.capture(), eq(10L), eq(TimeUnit.MILLISECONDS));

      // We capture the argument so we can run as if the scheduler worked and assert
      Runnable runnable = argument.getValue();
      runnable.run();

      assertTrue(booleanCompletableFuture.isDone());
      assertFalse(booleanCompletableFuture.get());
      assertEquals(0, expirationScheduler.countScheduledRequests());
   }

   @Test
   public void testScheduleForCompletionAddsASingleRequestById() throws Exception {

      expirationScheduler.scheduleForCompletion("123", booleanCompletableFuture, 100, TimeUnit.SECONDS);
      expectException(IllegalStateException.class, () -> expirationScheduler.scheduleForCompletion("123", booleanCompletableFuture, 50, TimeUnit.SECONDS));

      RequestExpirationScheduler.ScheduledRequest scheduledRequest = expirationScheduler.get("123");

      assertEquals(scheduledFutureMock, scheduledRequest.getScheduledFuture());
      assertEquals(booleanCompletableFuture, scheduledRequest.getRequest());
      assertEquals(1, expirationScheduler.countScheduledRequests());
      verify(scheduledExecutorService).schedule(any(Runnable.class), eq(100L), eq(TimeUnit.SECONDS));
      verify(scheduledExecutorService, never()).schedule(any(Runnable.class), eq(0L), eq(TimeUnit.SECONDS));
      verify(scheduledExecutorService, never()).schedule(any(Runnable.class), eq(50L), eq(TimeUnit.SECONDS));
   }

   @Test
   public void testCompletedRequestsShouldNotBeScheduled() throws Exception {

      CompletableFuture<Boolean> request = new CompletableFuture();
      request.complete(true);

      expirationScheduler.scheduleForCompletion("123", request, 10, TimeUnit.MILLISECONDS);
      assertEquals(0, expirationScheduler.countScheduledRequests());
   }

   @Test
   public void testAbortSchedulingWithCompletedRequest() throws Exception {

      expirationScheduler.scheduleForCompletion("123", booleanCompletableFuture, 42, TimeUnit.SECONDS);
      RequestExpirationScheduler.ScheduledRequest scheduledRequest = expirationScheduler.get("123");
      assertEquals(1, expirationScheduler.countScheduledRequests());

      booleanCompletableFuture.complete(true);

      expirationScheduler.abortScheduling("123");

      assertTrue(booleanCompletableFuture.isDone());
      assertTrue(booleanCompletableFuture.get());
      verify(scheduledFutureMock).cancel(false);
      assertEquals(0, expirationScheduler.countScheduledRequests());
   }

   @Test
   public void testAbortSchedulingShouldNotWorkIfRequestIsNotCompleted() throws Exception {

      expirationScheduler.scheduleForCompletion("123", booleanCompletableFuture, 42, TimeUnit.SECONDS);
      RequestExpirationScheduler.ScheduledRequest scheduledRequest = expirationScheduler.get("123");
      assertEquals(1, expirationScheduler.countScheduledRequests());

      expirationScheduler.abortScheduling("123");

      assertFalse(booleanCompletableFuture.isDone());
      verify(scheduledFutureMock, never()).cancel(false);
      assertEquals(1, expirationScheduler.countScheduledRequests());
   }

   @Test
   public void testAbortSchedulingShouldWorkIfRequestIsNotCompletedAndForce() throws Exception {

      expirationScheduler.scheduleForCompletion("123", booleanCompletableFuture, 42, TimeUnit.SECONDS);
      RequestExpirationScheduler.ScheduledRequest scheduledRequest = expirationScheduler.get("123");
      assertEquals(1, expirationScheduler.countScheduledRequests());

      expirationScheduler.abortScheduling("123", true);

      assertFalse(booleanCompletableFuture.isDone());
      verify(scheduledFutureMock).cancel(false);
      assertEquals(0, expirationScheduler.countScheduledRequests());
   }

   @Test
   public void testAbortSchedulingDoNothingForUnexistingRequests() throws Exception {

      expirationScheduler.abortScheduling("unexisting");
      assertEquals(0, expirationScheduler.countScheduledRequests());
   }
}
