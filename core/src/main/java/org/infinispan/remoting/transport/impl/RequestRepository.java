package org.infinispan.remoting.transport.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.time.TimeService;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.AbstractRequest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManager;
import org.infinispan.remoting.transport.jgroups.RequestTracker;
import org.infinispan.remoting.transport.jgroups.SingleSiteRequest;
import org.infinispan.remoting.transport.jgroups.StaggeredRequest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Registry of in-flight remote command requests.
 *
 * <p>
 * Tracks all pending {@link Request} instances, dispatches inbound responses to the corresponding request, and cancels
 * outstanding requests when the cache manager stops.
 * </p>
 *
 * <p>Thread-safety: All methods are safe for concurrent use.</p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class RequestRepository implements Lifecycle {
   private static final Log log = LogFactory.getLog(RequestRepository.class);

   private final ConcurrentHashMap<Long, Request<?, ?>> requests;
   private final AtomicLong nextRequestId = new AtomicLong(1);
   private final JGroupsMetricsManager metricsManager;
   private final ScheduledExecutorService timeoutExecutor;
   private final TimeService timeService;

   private volatile boolean running = true;

   public RequestRepository(JGroupsMetricsManager metricsManager, ScheduledExecutorService timeoutExecutor,
                            TimeService timeService) {
      this.requests = new ConcurrentHashMap<>();
      this.metricsManager = metricsManager;
      this.timeoutExecutor = timeoutExecutor;
      this.timeService = timeService;
   }

   private long newRequestId() {
      long requestId = nextRequestId.getAndIncrement();
      // Make sure NO_REQUEST_ID is never used for a request
      if (requestId == Request.NO_REQUEST_ID) {
         requestId = nextRequestId.getAndIncrement();
      }
      return requestId;
   }

   /**
    * Creates a request targeting and expecting a response from a single cluster member.
    *
    * <p>
    * If the repository has been stopped, the returned request is immediately cancelled. If the target destination has
    * enough backlog of pending requests, the request is cancelled immediately. Otherwise, if a timeout is provided,
    * the requests will be scheduled to timeout with the appropriate allowance time based on the destination backlog.
    * </p>
    *
    * @param <T>       the result type produced by the collector
    * @param target    the address of the single expected respondent
    * @param flags     flags associated with the command
    * @param collector gathers the response and produces the final result
    * @param timeout   timeout duration; if positive and a timeout executor is available, the request is cancelled with
    *                  a {@link org.infinispan.commons.TimeoutException} on expiry
    * @param unit      time unit for {@code timeout}
    * @return a completable request that completes when the collector produces a result
    */
   public <T> Request<Address, T> singleRequest(Address target, long flags, ResponseCollector<Address, T> collector, long timeout, TimeUnit unit) {
      RequestTracker tracker = metricsManager.trackRequest(target, flags);
      SingleTargetRequest<T> request = new SingleTargetRequest<>(collector, newRequestId(), this, tracker);

      if (timeout > 0 && tracker.shouldShed()) {
         request.cancel(CONTAINER.requestShed(target, false));
         return request;
      }

      if (timeout > 0 && timeoutExecutor != null) {
         long adjustedTimeoutNs = tracker.adjustTimeout(unit.toNanos(timeout));
         return handleRequest(request, adjustedTimeoutNs, TimeUnit.NANOSECONDS);
      }
      return handleRequest(request, timeout, unit);
   }

   /**
    * Creates a request expecting responses from multiple cluster members.
    *
    * <p>
    * If all targets are excluded or absent, the returned request may already be complete. If the repository has been stopped,
    * the returned request is immediately cancelled.
    * </p>
    *
    * @param <T>       the result type produced by the collector
    * @param targets   the addresses of the expected respondents
    * @param excluded  address to exclude from the target set (typically the local node), or {@code null}
    * @param collector gathers responses and produces the final result
    * @param timeout   timeout duration; if positive and a timeout executor is available, the request is cancelled with
    *                  a {@link org.infinispan.commons.TimeoutException} on expiry
    * @param unit      time unit for {@code timeout}
    * @return a completable request that completes when the collector produces a result
    */
   public <T> Request<Address, T> multiRequest(Collection<Address> targets, Address excluded, ResponseCollector<Address, T> collector,
                                               long timeout, TimeUnit unit) {
      MultiTargetRequest<T> request = new MultiTargetRequest<>(collector, newRequestId(), this, targets, excluded, metricsManager);
      if (request.isDone())
         return request;

      return handleRequest(request, timeout, unit);
   }

   /**
    * Creates a request expecting a response from a single remote site (cross-site replication).
    *
    * <p>
    * If the repository has been stopped, the returned request is immediately cancelled.
    * </p>
    *
    * @param <T>       the result type produced by the collector
    * @param site      the name of the target site
    * @param collector gathers the response and produces the final result
    * @param timeout   timeout duration; if positive and a timeout executor is available, the request
    *                  is cancelled with a {@link org.infinispan.commons.TimeoutException} on expiry
    * @param unit      time unit for {@code timeout}
    * @return a completable request that completes when the collector produces a result
    */
   public <T> Request<String, T> singleSiteRequest(String site, ResponseCollector<String, T> collector, long timeout, TimeUnit unit) {
      SingleSiteRequest<T> request = new SingleSiteRequest<>(collector, newRequestId(), this, site);
      return handleRequest(request, timeout, unit);
   }

   /**
    * Creates a request that sends the command to targets one at a time, advancing to the next target on each response
    * or stagger-timeout.
    *
    * <p>
    * Unlike the other factory methods, the returned request manages its own timeout scheduling and message dispatch.
    * If the repository has been stopped, the request is immediately cancelled.
    * </p>
    *
    * @param <T>       the result type produced by the collector
    * @param targets   the candidate respondents, tried in order
    * @param excluded  address to exclude (typically the local node), or {@code null}
    * @param collector gathers responses and produces the final result
    * @param sender    the callback responsible for sending the request
    * @param timeout   overall timeout for the entire staggered sequence
    * @param unit      time unit for {@code timeout}
    * @return the staggered request; the caller is responsible for initiating the first send
    */
   public <T> StaggeredRequest<T> staggeredRequest(Collection<Address> targets, Address excluded, ResponseCollector<Address, T> collector,
                                                   StaggeredRequest.StaggeredSender sender, long timeout, TimeUnit unit) {
      StaggeredRequest<T> request = new StaggeredRequest<>(collector, newRequestId(), this, targets, excluded,
            metricsManager, timeService, timeoutExecutor, sender, timeout, unit);
      addRequest(request);

      if (!running)
         request.cancel(CONTAINER.cacheManagerIsStopping());

      return request;
   }

   private <A, B> Request<A, B> handleRequest(AbstractRequest<A, B> request, long timeout, TimeUnit unit) {
      addRequest(request);

      if (!running)
         request.cancel(CONTAINER.cacheManagerIsStopping());

      if (timeout > 0 && timeoutExecutor != null)
         request.setTimeout(timeoutExecutor, timeout, unit);

      return request;
   }

   protected void addRequest(Request<?, ?> request) {
      long requestId = request.getRequestId();
      Request<?, ?> existing = requests.put(requestId, request);
      if (existing != null) {
         throw new IllegalStateException(String.format("Duplicate request id (%d): curr=%s, prev=%s", requestId, request, existing));
      }
   }

   /**
    * Dispatches an inbound response to the request that originated it.
    *
    * <p>
    * If no request with the given id exists (already completed or unknown), the response is silently ignored.
    * </p>
    *
    * @param requestId the request identifier carried by the response message
    * @param sender    the origin of the response (an {@link Address} or site name)
    * @param response  the response payload
    */
   public final void addResponse(long requestId, Object sender, Response response) {
      @SuppressWarnings("unchecked")
      Request<Object, ?> request = (Request<Object, ?>) requests.get(requestId);
      if (request == null) {
         if (log.isTraceEnabled())
            log.tracef("Ignoring response for non-existent request %d from %s: %s", requestId, sender, response);
         return;
      }
      request.onResponse(sender, response);
   }

   /**
    * Removes a request from the registry.
    *
    * <p>
    * Typically called by the request itself upon completion, cancellation, or timeout.
    * </p>
    *
    * @param requestId the identifier of the request to remove
    */
   public void removeRequest(long requestId) {
      requests.remove(requestId);
   }

   /**
    * Applies the given action to every in-flight request.
    *
    * @param consumer the action to apply
    */
   public void forEach(Consumer<Request<?, ?>> consumer) {
      requests.forEach((id, request) -> consumer.accept(request));
   }

   @Override
   public void start() { }

   /**
    * Marks the repository as stopped so that newly created requests are immediately cancelled.
    *
    * <p>
    * Does not cancel already-registered requests; the caller is responsible for draining those separately via
    * {@link #forEach(Consumer)}.
    * </p>
    */
   @Override
   public void stop() {
      running = false;
   }
}
