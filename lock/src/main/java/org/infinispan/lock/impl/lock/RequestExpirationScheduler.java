package org.infinispan.lock.impl.lock;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.lock.logging.Log;

/**
 * This class holds the logic to schedule/abort requests that need to be completed at a given time.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class RequestExpirationScheduler {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ScheduledExecutorService scheduledExecutorService;
   private final ConcurrentMap<String, ScheduledRequest> scheduledRequests = new ConcurrentHashMap<>();

   public RequestExpirationScheduler(ScheduledExecutorService scheduledExecutorService) {
      this.scheduledExecutorService = scheduledExecutorService;
   }

   class ScheduledRequest {
      private CompletableFuture<Boolean> request;
      private ScheduledFuture<?> scheduledFuture;

      public ScheduledRequest(CompletableFuture<Boolean> request, ScheduledFuture<?> scheduledFuture) {
         this.request = request;
         this.scheduledFuture = scheduledFuture;
      }

      public CompletableFuture<Boolean> getRequest() {
         return request;
      }

      public ScheduledFuture<?> getScheduledFuture() {
         return scheduledFuture;
      }
   }

   /**
    * Schedules a request for completion
    *
    * @param requestId, the unique identifier if the request
    * @param request,   the request
    * @param time,      time expressed in long
    * @param unit,      {@link TimeUnit}
    */
   public void scheduleForCompletion(String requestId, CompletableFuture<Boolean> request, long time, TimeUnit unit) {
      if (request.isDone()) {
         if (trace) {
            log.tracef("Request[%s] is not scheduled because is already done", requestId);
         }
         return;
      }

      if (scheduledRequests.containsKey(requestId)) {
         String message = String.format("Request[%s] is not scheduled because it is already scheduled", requestId);
         log.error(message);
         throw new IllegalStateException(message);
      }

      if (trace) {
         log.tracef("Request[%s] being scheduled to be completed in [%d, %s]", requestId, time, unit);
      }

      ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(() -> {
         request.complete(false);
         scheduledRequests.remove(requestId);
      }, time, unit);

      scheduledRequests.putIfAbsent(requestId, new ScheduledRequest(request, scheduledFuture));
   }

   /**
    * Aborts the scheduled request if the request is already completed
    *
    * @param requestId, unique identifier of the request
    */
   public void abortScheduling(String requestId) {
      abortScheduling(requestId, false);
   }

   /**
    * Aborts the scheduled request. If force is true, it will abort even if the request is not completed
    *
    * @param requestId, unique identifier of the request
    * @param force,     force abort
    */
   public void abortScheduling(String requestId, boolean force) {
      if (trace) {
         log.tracef("Request[%s] abort scheduling", requestId);
      }
      ScheduledRequest scheduledRequest = scheduledRequests.get(requestId);
      if (scheduledRequest != null && (scheduledRequest.request.isDone() || force)) {
         scheduledRequest.scheduledFuture.cancel(false);
         scheduledRequests.remove(requestId);
      }
   }

   /**
    * Returns the size of the currently scheduled requests
    *
    * @return the number of requests that are pending on the scheduler
    */
   public int countScheduledRequests() {
      return scheduledRequests.size();
   }

   /**
    * Get scheduled request reference by id if such exist
    *
    * @param requestId, the id of the scheduled request
    * @return {@link ScheduledRequest} the request
    */
   public ScheduledRequest get(String requestId) {
      return scheduledRequests.get(requestId);
   }

   /**
    * Clears all the scheduled requests
    */
   public void clear() {
      scheduledRequests.clear();
   }
}
