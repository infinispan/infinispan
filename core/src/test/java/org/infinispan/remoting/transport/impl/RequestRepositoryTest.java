package org.infinispan.remoting.transport.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.remoting.responses.SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.util.concurrent.LifecycleRejectedExecutionHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.BaseJGroupsMetricManager;
import org.infinispan.remoting.transport.jgroups.RequestTracker;
import org.infinispan.remoting.transport.jgroups.StaggeredRequest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "remoting.transport.impl.RequestRepositoryTest")
public class RequestRepositoryTest {

   private ScheduledExecutorService timeoutExecutor;

   /**
    * Creates an executor service mimicking the cache manager's timeout executor: once shut down it rejects new tasks by throwing {@link IllegalLifecycleStateException}.
    */
   private static ScheduledExecutorService newTimeoutExecutor() {
      ScheduledThreadPoolExecutor result = new ScheduledThreadPoolExecutor(1);
      result.setRejectedExecutionHandler(LifecycleRejectedExecutionHandler.getInstance());
      return result;
   }

   @AfterMethod
   public void tearDown() {
      if (timeoutExecutor != null) {
         timeoutExecutor.shutdownNow();
      }
   }

   private static BaseJGroupsMetricManager metricsManager() {
      BaseJGroupsMetricManager manager = new BaseJGroupsMetricManager();
      TestingUtil.replaceField(new ControlledTimeService(), "timeService", manager, BaseJGroupsMetricManager.class);
      return manager;
   }

   public void testSingleRequestRegistersAndReceivesResponse() throws Exception {
      Address target = Address.random();
      RequestRepository repository = new RequestRepository(metricsManager(), null, null);

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testSingleRequestTimesOutAfterSpecifiedDuration() {
      Address target = Address.random();
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
      RequestRepository repository = new RequestRepository(metricsManager(), timeoutExecutor, null);

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 1, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);
   }

   public void testSingleRequestWithZeroTimeoutDoesNotTimeout() throws Exception {
      Address target = Address.random();
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
      RequestRepository repository = new RequestRepository(metricsManager(), timeoutExecutor, null);

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      assertThat(request.toCompletableFuture()).isNotDone();

      repository.addResponse(request.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testSingleRequestCancelledWhenNotRunning() {
      Address target = Address.random();
      RequestRepository repository = new RequestRepository(metricsManager(), null, null);
      repository.stop();

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      assertThat(request.toCompletableFuture())
            .isCompletedExceptionally();
      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalLifecycleStateException.class);
   }

   public void testSingleRequestShedWhenDestinationBacklogged() {
      Address target = Address.random();
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
      BaseJGroupsMetricManager metricsManager = metricsManager();

      // Degrade the destination: consecutive timeouts shrink its adaptive concurrency limit.
      for (int i = 0; i < 4; i++) {
         metricsManager.trackRequest(target, 0L).resolve(RequestTracker.Outcome.TIMEOUT);
      }

      // Pile the in-flight count past the shrunken limit before the request under test.
      for (int i = 0; i < 20_000; i++) {
         metricsManager.trackRequest(target, 0L);
      }

      RequestRepository repository = new RequestRepository(metricsManager, timeoutExecutor, new ControlledTimeService());

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, 0L, SingleResponseCollector.validOnly(), 1, TimeUnit.SECONDS);

      assertThat(request.toCompletableFuture())
            .isCompletedExceptionally();
      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);

      // Assert the operation is still submitted if the timeout is 0.
      Request<Address, ValidResponse<?>> request2 = repository.singleRequest(target, 0L, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);
      assertThat(request2.toCompletableFuture().isDone()).isFalse();
      repository.addResponse(request2.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);
      assertThat(request2.toCompletableFuture().isDone()).isTrue();
   }

   public void testMultiRequestRegistersAndReceivesResponses() throws Exception {
      Address self = Address.random();
      Address target1 = Address.random();
      Address target2 = Address.random();
      RequestRepository repository = new RequestRepository(metricsManager(), null, null);

      Request<Address, Map<Address, Response>> request = repository.multiRequest(
            List.of(self, target1, target2), self,
            MapResponseCollector.ignoreLeavers(3), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), target1, SUCCESSFUL_EMPTY_RESPONSE);
      repository.addResponse(request.getRequestId(), target2, SUCCESSFUL_EMPTY_RESPONSE);

      Map<Address, Response> result = request.toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertThat(result)
            .containsOnlyKeys(target1, target2)
            .containsEntry(target1, SUCCESSFUL_EMPTY_RESPONSE)
            .containsEntry(target2, SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testMultiRequestAllExcludedCompletesImmediately() throws Exception {
      Address self = Address.random();
      RequestRepository repository = new RequestRepository(metricsManager(), null, null);

      Request<Address, Map<Address, Response>> request = repository.multiRequest(
            List.of(self), self,
            MapResponseCollector.ignoreLeavers(1), 0, TimeUnit.MILLISECONDS);

      assertThat(request.toCompletableFuture())
            .isDone();
      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isEmpty();

      // Not registered, a response for its ID is silently ignored
      repository.addResponse(request.getRequestId(), self, SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testStaggeredRequestDoesNotScheduleExternalTimeout() throws Exception {
      Address target1 = Address.random();
      Address target2 = Address.random();
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();

      RequestRepository repository = new RequestRepository(metricsManager(), timeoutExecutor,
            new ControlledTimeService());

      // Single-target with 1ms timeout will time out
      Request<Address, ValidResponse<?>> singleRequest = repository.singleRequest(
            target1, 0L, SingleResponseCollector.validOnly(), 1, TimeUnit.MILLISECONDS);

      // Staggered with same timeout, factory must NOT schedule timeout
      StaggeredRequest<Map<Address, Response>> staggeredRequest = repository.staggeredRequest(
            List.of(target2), null, MapResponseCollector.ignoreLeavers(),
            (dest, reqId) -> {}, 1, TimeUnit.MILLISECONDS);

      // Wait for single-target to time out
      assertThatThrownBy(() -> singleRequest.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);

      // Staggered is still pending, no external timeout was scheduled
      assertThat(staggeredRequest.toCompletableFuture()).isNotDone();
   }

   public void testSingleSiteRequestRegistersAndTimesOut() {
      String site = "NYC";
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
      RequestRepository repository = new RequestRepository(metricsManager(), timeoutExecutor, null);

      Request<String, ValidResponse<?>> request = repository.singleSiteRequest(
            site, SingleResponseCollector.validOnly(), 1, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);
   }

   public void testSingleSiteRequestRegistersAndReceivesResponse() throws Exception {
      String site = "NYC";
      RequestRepository repository = new RequestRepository(metricsManager(), null, null);

      Request<String, ValidResponse<?>> request = repository.singleSiteRequest(
            site, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), site, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }

   /**
    * Reproduces the production race: a request is created after both the repository has stopped and its timeout
    * executor service has been shut down (as happens while stopping cache managers/transports). Creating the request
    * must not throw, it must simply return an already-cancelled request.
    */
   public void testSingleSiteRequestDoesNotThrowAfterStopWithShutDownExecutor() {
      String site = "NYC";
      timeoutExecutor = newTimeoutExecutor();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor, null);

      // Stop the repository and shut down its executor service as happens when stopping a cache manager/transport.
      repository.stop();
      timeoutExecutor.shutdown();

      Request<String, ValidResponse<?>> request = repository.singleSiteRequest(
            site, SingleResponseCollector.validOnly(), 500, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalLifecycleStateException.class);
   }

   public void testMultiRequestDoesNotThrowAfterStopWithShutDownExecutor() {
      Address self = Address.random();
      Address target = Address.random();
      timeoutExecutor = newTimeoutExecutor();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor, null);

      repository.stop();
      timeoutExecutor.shutdown();

      Request<Address, Map<Address, Response>> request = repository.multiRequest(
            List.of(self, target), self,
            MapResponseCollector.ignoreLeavers(2), 500, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalLifecycleStateException.class);
   }

   /**
    * Reproduces the concurrent stop race: the executor service is shut down while requests are still being created.
    * Scheduling the timeout task must fail gracefully and cancel the request instead of throwing from the factory method.
    */
   public void testSingleSiteRequestCancelledWhenTimeoutExecutorShutsDownConcurrently() {
      String site = "NYC";
      timeoutExecutor = newTimeoutExecutor();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor, null);

      // Shut down the executor while the repository is still running: schedule() will be rejected.
      timeoutExecutor.shutdown();

      Request<String, ValidResponse<?>> request = repository.singleSiteRequest(
            site, SingleResponseCollector.validOnly(), 500, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalLifecycleStateException.class);
   }
}
