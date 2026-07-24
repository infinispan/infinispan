package org.infinispan.remoting.transport.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.remoting.responses.SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.NoOpJGroupsMetricManager;
import org.infinispan.remoting.transport.jgroups.StaggeredRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "remoting.transport.impl.RequestRepositoryTest")
public class RequestRepositoryTest {

   private ScheduledExecutorService timeoutExecutor;

   @AfterMethod
   public void tearDown() {
      if (timeoutExecutor != null) {
         timeoutExecutor.shutdownNow();
      }
   }

   public void testSingleRequestRegistersAndReceivesResponse() throws Exception {
      Address target = Address.random();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null);

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testSingleRequestTimesOutAfterSpecifiedDuration() {
      Address target = Address.random();
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor, null);

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, SingleResponseCollector.validOnly(), 1, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);
   }

   public void testSingleRequestWithZeroTimeoutDoesNotTimeout() throws Exception {
      Address target = Address.random();
      timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor, null);

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      assertThat(request.toCompletableFuture()).isNotDone();

      repository.addResponse(request.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testSingleRequestCancelledWhenNotRunning() {
      Address target = Address.random();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null);
      repository.stop();

      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      assertThat(request.toCompletableFuture())
            .isCompletedExceptionally();
      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalLifecycleStateException.class);
   }

   public void testMultiRequestRegistersAndReceivesResponses() throws Exception {
      Address self = Address.random();
      Address target1 = Address.random();
      Address target2 = Address.random();
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null);

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
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null);

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

      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor,
            new ControlledTimeService());

      // Single-target with 1ms timeout will time out
      Request<Address, ValidResponse<?>> singleRequest = repository.singleRequest(
            target1, SingleResponseCollector.validOnly(), 1, TimeUnit.MILLISECONDS);

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
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, timeoutExecutor, null);

      Request<String, ValidResponse<?>> request = repository.singleSiteRequest(
            site, SingleResponseCollector.validOnly(), 1, TimeUnit.MILLISECONDS);

      assertThatThrownBy(() -> request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(TimeoutException.class);
   }

   public void testSingleSiteRequestRegistersAndReceivesResponse() throws Exception {
      String site = "NYC";
      RequestRepository repository = new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null);

      Request<String, ValidResponse<?>> request = repository.singleSiteRequest(
            site, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), site, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(10, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }
}
