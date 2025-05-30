package org.infinispan.remoting.transport.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManager;
import org.infinispan.remoting.transport.jgroups.RequestTracker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Request implementation that waits for responses from multiple target nodes.
 * @author Dan Berindei
 * @since 9.1
 */
public class MultiTargetRequest<T> extends ExclusiveTargetRequest<T> {
   private static final Log log = LogFactory.getLog(MultiTargetRequest.class);

   private final RequestTracker[] trackers;
   private int missingResponses;

   public MultiTargetRequest(ResponseCollector<Address, T> responseCollector, long requestId, RequestRepository repository,
                             Collection<Address> targets, Address excluded, JGroupsMetricsManager metricsCollector) {
      super(responseCollector, requestId, repository);
      trackers = new RequestTracker[targets.size()];
      int i = 0;
      for (Address target : targets) {
         if (excluded == null || !excluded.equals(target)) {
            trackers[i++] = metricsCollector.trackRequest(target);
         }
      }
      missingResponses = i;
      if (missingResponses == 0)
         complete(responseCollector.finish());
   }

   protected int getTargetsSize() {
      return trackers.length;
   }

   /**
    * @return target {@code i}, or {@code null} if a response was already added for target {@code i}.
    */
   protected RequestTracker getTarget(int i) {
      return trackers[i];
   }

   @Override
   protected void actualOnResponse(Address sender, Response response) {
      try {
         boolean isDone = false;

         if (missingResponses <= 0) {
            // The request is completed, nothing to do
            return;
         }

         boolean invalidSender = true;
         for (int i = 0; i < trackers.length; i++) {
            RequestTracker target = trackers[i];
            if (target != null && target.destination().equals(sender)) {
               target.onComplete();
               invalidSender = false;
               trackers[i] = null;
               missingResponses--;
               break;
            }
         }

         if (invalidSender) {
            // A broadcast may be sent to nodes added to the cluster view after the request was created,
            // so we should just ignore responses from unexpected senders.
            if (log.isTraceEnabled())
               log.tracef("Ignoring unexpected response to request %d from %s: %s", requestId, sender, response);
            return;
         }

         T result = responseCollector.addResponse(sender, response);
         if (result != null) {
            isDone = true;
            // Make sure to ignore any other responses
            missingResponses = 0;
         } else if (missingResponses <= 0) {
            isDone = true;
            result = responseCollector.finish();
         }

         if (isDone) {
            complete(result);
         }
      } catch (Throwable t) {
         completeExceptionally(t);
      }
   }

   @Override
   protected boolean actualOnView(Set<Address> members) {

      boolean targetRemoved = false;
      try {
         boolean isDone = false;
         T result = null;
         if (missingResponses <= 0) {
            // The request is completed, must not modify ResponseObject.
            return false;
         }
         for (int i = 0; i < trackers.length; i++) {
            RequestTracker target = trackers[i];
            if (target != null && !members.contains(target.destination())) {
               trackers[i] = null;
               missingResponses--;
               targetRemoved = true;
               if (log.isTraceEnabled())
                  log.tracef("Target %s of request %d left the cluster view", target, requestId);
               result = responseCollector.addResponse(target.destination(), CacheNotFoundResponse.INSTANCE);
               if (result != null) {
                  isDone = true;
                  break;
               }
            }
         }

         // No more targets remaining
         if (!isDone && missingResponses <= 0) {
            result = responseCollector.finish();
            isDone = true;
         }

         // Complete the request outside the lock, in case it has to run blocking callbacks
         if (isDone) {
            complete(result);
         }
      } catch (Throwable t) {
         completeExceptionally(t);
      }
      return targetRemoved;
   }

   @Override
   protected void actualOnTimeout() {
      String targetsWithoutResponses;
      if (missingResponses <= 0) {
         // The request is already completed.
         return;
      }
      // Don't add more responses to the collector after this
      missingResponses = 0;
      targetsWithoutResponses = Arrays.stream(trackers)
            .filter(Objects::nonNull)
            .peek(RequestTracker::onTimeout)
            .map(RequestTracker::destination)
            .map(Object::toString)
            .collect(Collectors.joining(","));
      completeExceptionally(CLUSTER.requestTimedOut(requestId, targetsWithoutResponses, Util.prettyPrintTime(getTimeoutMs())));
   }
}
