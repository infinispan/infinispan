package org.infinispan.test.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.remoting.responses.SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
import static org.mockito.Mockito.spy;

import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.Request;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.jgroups.NoOpJGroupsMetricManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Verifies the two guarantees of {@link StateResponseHolder}: a held response never blocks the thread that delivers
 * it, and a held response is never dropped, even when nobody releases it.
 */
@Test(groups = "unit", testName = "test.transport.StateResponseHolderTest")
public class StateResponseHolderTest extends AbstractInfinispanTest {

   private static final long TIMEOUT_SECONDS = 10;

   public void testHeldResponseDoesNotBlockCallerAndIsDeliveredOnRelease() throws Exception {
      RequestRepository repository = spy(new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null));
      StateResponseHolder holder = StateResponseHolder.install(repository, "holder", Response::isSuccessful,
            testExecutor(), TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Address target = Address.random();
      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      // Reaching the next line at all shows addResponse returned to its caller while the response is held.
      repository.addResponse(request.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);

      holder.awaitHeld(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertThat(request.toCompletableFuture()).isNotDone();

      holder.release();
      assertThat(request.toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
   }

   public void testHeldResponseIsDeliveredWhenHoldExpires() throws Exception {
      RequestRepository repository = spy(new RequestRepository(NoOpJGroupsMetricManager.INSTANCE, null, null));
      // Nobody releases the response, so a short hold keeps the test fast.
      StateResponseHolder holder = StateResponseHolder.install(repository, "holder", Response::isSuccessful,
            testExecutor(), 10, TimeUnit.MILLISECONDS);
      Address target = Address.random();
      Request<Address, ValidResponse<?>> request = repository.singleRequest(
            target, SingleResponseCollector.validOnly(), 0, TimeUnit.MILLISECONDS);

      repository.addResponse(request.getRequestId(), target, SUCCESSFUL_EMPTY_RESPONSE);

      assertThat(request.toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
            .isSameAs(SUCCESSFUL_EMPTY_RESPONSE);
      // A release after the hold expired means the test lost control of the timing it relies on.
      assertThatThrownBy(holder::release).isInstanceOf(AssertionError.class);
   }
}
