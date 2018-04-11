package org.infinispan.remoting.transport.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.AbstractRequest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * Request implementation that waits for responses from multiple target nodes.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class MultiTargetRequest<T> extends AbstractRequest<T> {
   private static final Log log = LogFactory.getLog(MultiTargetRequest.class);
   private static final boolean trace = log.isTraceEnabled();

   @GuardedBy("responseCollector")
   private final Address[] targets;
   @GuardedBy("responseCollector")
   private int missingResponses;

   public MultiTargetRequest(ResponseCollector<T> responseCollector, long requestId, RequestRepository repository,
                             Collection<Address> targets, Address excluded) {
      super(requestId, responseCollector, repository);
      this.targets = new Address[targets.size()];
      int i = 0;
      for (Address target : targets) {
         if (excluded == null || !excluded.equals(target)) {
            this.targets[i++] = target;
         }
      }
      this.missingResponses = i;
      if (missingResponses == 0)
         complete(responseCollector.finish());
   }

   protected int getTargetsSize() {
      return targets.length;
   }

   /**
    * @return target {@code i}, or {@code null} if a response was already added for target {@code i}.
    */
   protected Address getTarget(int i) {
      return targets[i];
   }

   @Override
   public void onResponse(Address sender, Response response) {
      try {
         boolean isDone = false;
         T result;

         synchronized (responseCollector) {
            if (missingResponses <= 0) {
               // The request is completed, nothing to do
               return;
            }

            boolean validSender = false;
            for (int i = 0; i < targets.length; i++) {
               Address target = targets[i];
               if (target != null && target.equals(sender)) {
                  validSender = true;
                  targets[i] = null;
                  missingResponses--;
                  break;
               }
            }

            if (!validSender) {
               // A broadcast may be sent to nodes added to the cluster view after the request was created,
               // so we should just ignore responses from unexpected senders.
               if (trace)
                  log.tracef("Ignoring unexpected response to request %d from %s: %s", requestId, sender, response);
               return;
            }

            result = responseCollector.addResponse(sender, response);
            if (result != null) {
               isDone = true;
            } else if (missingResponses <= 0) {
               isDone = true;
               result = responseCollector.finish();
            }
         }

         // Complete the request outside the lock, in case it has to run blocking callbacks
         if (isDone) {
            complete(result);
         }
      } catch (Throwable t) {
         completeExceptionally(t);
      }
   }

   @Override
   public boolean onNewView(Set<Address> members) {
      boolean targetRemoved = false;
      try {
         boolean isDone = false;
         T result = null;
         synchronized (responseCollector) {
            if (missingResponses <= 0) {
               // The request is completed, must not modify ResponseObject.
               return false;
            }
            for (int i = 0; i < targets.length; i++) {
               Address target = targets[i];
               if (target != null && !members.contains(target)) {
                  targets[i] = null;
                  missingResponses--;
                  targetRemoved = true;
                  if (trace) log.tracef("Target %s of request %d left the cluster view", target, requestId);
                  result = responseCollector.addResponse(target, CacheNotFoundResponse.INSTANCE);
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
   protected void onTimeout() {
      synchronized (responseCollector) {
         if (missingResponses <= 0) {
            // The request is already completed.
            return;
         }
         // Don't add more responses to the collector after this
         this.missingResponses = 0;
      }

      String targetsWithoutResponses = Arrays.stream(targets)
                                             .filter(Objects::nonNull)
                                             .map(Object::toString)
                                             .collect(Collectors.joining(","));
      completeExceptionally(log.requestTimedOut(requestId, targetsWithoutResponses));
   }
}
