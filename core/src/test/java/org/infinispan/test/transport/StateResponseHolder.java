package org.infinispan.test.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulObjResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.invocation.InvocationOnMock;

/**
 * Holds back the state transfer responses received by a cache manager, so a test can pause state transfer at a known
 * point and resume it later.
 *
 * <p>
 * State transfer data arrives as publisher responses, and the holder holds every publisher response the manager
 * receives, including those of any other cluster-wide iteration. A test must therefore not run its own cluster-wide
 * iteration from that manager while responses are held, or that iteration waits for the release as well.
 * </p>
 *
 * <p>
 * The thread that receives a state response is never blocked: it continues at once and the response is kept aside.
 * Blocking that thread instead would also hold back every message delivered after the response in the same JGroups
 * batch, which can deadlock the cluster under test.
 * </p>
 *
 * <p>
 * Every held response is delivered, either on {@link #release()} or when the hold timeout expires. A response is
 * never dropped, because a dropped response leaves its request pending until the RPC timeout and makes state transfer
 * give up on the segments it carried. Other responses are delivered unchanged.
 * </p>
 */
public final class StateResponseHolder {

   private static final Predicate<Response> PUBLISHER_RESPONSE = r ->
         r.isSuccessful() && r instanceof SuccessfulObjResponse<?> sor && sor.getResponseValue() instanceof PublisherResponse;

   private static final String HELD = "held";
   private static final String RELEASE = "release";
   private static final String DELIVERED = "delivered";

   private final CheckPoint checkPoint;
   private final Executor executor;
   private final long holdTimeoutNanos;
   private volatile boolean released;

   private StateResponseHolder(String name, Executor executor, long holdTimeout, TimeUnit unit) {
      // The name prefixes the CheckPoint's logs and timeout messages, telling holders on different nodes apart.
      this.checkPoint = new CheckPoint(name);
      this.executor = Objects.requireNonNull(executor);
      this.holdTimeoutNanos = unit.toNanos(holdTimeout);
   }

   /**
    * Starts holding the state transfer responses received by the transport of {@code manager}.
    *
    * <p>
    * From this call on, every state response is held until {@link #release()} is called or {@code holdTimeout}
    * expires, whichever comes first. State responses received after the release are delivered without delay.
    * </p>
    *
    * @param manager     the cache manager whose transport receives the responses; holding applies to every cache of
    *                    this manager
    * @param executor    runs the hold and the deferred delivery; it must run each task on a thread other than the
    *                    caller's, such as the test's {@code testExecutor()}, because an executor that runs tasks on
    *                    the calling thread blocks the receiving thread and defeats the holder
    * @param holdTimeout the longest a response stays held without a release
    * @param unit        the unit of {@code holdTimeout}
    * @return the holder controlling the held responses
    * @throws NullPointerException if {@code executor} is {@code null}
    */
   public static StateResponseHolder install(EmbeddedCacheManager manager, Executor executor, long holdTimeout,
                                             TimeUnit unit) {
      Transport transport = TestingUtil.extractGlobalComponent(manager, Transport.class);
      RequestRepository repository = Mocks.replaceFieldWithSpy(transport, "requests");
      return install(repository, String.valueOf(manager.getAddress()), PUBLISHER_RESPONSE, executor, holdTimeout, unit);
   }

   /**
    * Starts holding the responses that match {@code matcher} on an existing repository spy, for tests that build the
    * repository without a cache manager.
    *
    * @param repositorySpy a Mockito spy of the repository to intercept
    * @param name          identifies this holder in logs and timeout messages
    * @param matcher       selects the responses to hold; any other response is delivered unchanged
    * @param executor      runs the hold and the deferred delivery; it must run each task on a thread other than
    *                      the caller's
    * @param holdTimeout   the longest a response stays held without a release
    * @param unit          the unit of {@code holdTimeout}
    * @return the holder controlling the held responses
    * @throws NullPointerException if {@code executor} is {@code null}
    */
   static StateResponseHolder install(RequestRepository repositorySpy, String name, Predicate<Response> matcher,
                                      Executor executor, long holdTimeout, TimeUnit unit) {
      StateResponseHolder holder = new StateResponseHolder(name, executor, holdTimeout, unit);
      doAnswer(invocation -> {
         if (holder.released) {
            // Nothing to hold any more: deliver on the receiving thread, as without the holder.
            return invocation.callRealMethod();
         }
         holder.executor.execute(() -> holder.holdAndDeliver(invocation));
         // addResponse is void; returning lets the receiving thread move on to the next message.
         return null;
      }).when(repositorySpy).addResponse(anyLong(), any(), argThat(matcher::test));
      return holder;
   }

   /**
    * Waits until one more response is being held.
    *
    * <p>
    * Each call consumes one held response: the n-th call returns once n responses have been held in total.
    * </p>
    *
    * @param timeout the longest time to wait
    * @param unit    the unit of {@code timeout}
    * @throws TimeoutException     if no further response was held within the timeout
    * @throws InterruptedException if the calling thread is interrupted while waiting
    */
   public void awaitHeld(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
      checkPoint.awaitStrict(HELD, timeout, unit);
   }

   /**
    * Delivers every held response and stops holding new ones.
    *
    * <p>
    * Calling it more than once has no further effect.
    * </p>
    *
    * @throws AssertionError if a held response was already delivered because the hold timeout expired before the
    *                        first call; every later call fails the same way
    */
   public void release() {
      // Set before releasing the held ones, so responses arriving from now on skip the executor.
      released = true;
      try {
         checkPoint.triggerForever(RELEASE);
      } catch (IllegalStateException e) {
         // CheckPoint throws when a waiter already timed out on RELEASE.
         throw new AssertionError("A held response was delivered on hold timeout, before release() was called", e);
      }
   }

   /**
    * Signals the hold, waits for the release or the hold timeout, then delivers the response to the real repository.
    *
    * <p>
    * Runs on the executor. The response is delivered in every case, including on timeout, on a failed signal and on
    * interruption, so its request always completes.
    * </p>
    *
    * @param invocation the intercepted {@code addResponse} call to complete
    */
   private void holdAndDeliver(InvocationOnMock invocation) {
      try {
         checkPoint.trigger(HELD);
         checkPoint.awaitStrict(RELEASE, holdTimeoutNanos, TimeUnit.NANOSECONDS);
      } catch (TimeoutException | IllegalStateException e) {
         // The hold expired, or the test already gave up in awaitHeld. Deliver anyway; on expiry, CheckPoint
         // remembers the timeout and release() fails.
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      Mocks.callRealMethod(invocation);
      checkPoint.trigger(DELIVERED);
   }
}
