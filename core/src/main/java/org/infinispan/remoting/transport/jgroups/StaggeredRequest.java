package org.infinispan.remoting.transport.jgroups;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.impl.MultiTargetRequest;
import org.infinispan.remoting.transport.impl.RequestRepository;

/**
 * @author Dan Berindei
 * @since 9.1
 */
public class StaggeredRequest<T> extends MultiTargetRequest<T> {
   private final StaggeredSender sender;
   private final ScheduledExecutorService timeoutExecutor;
   private final TimeService timeService;
   private final long deadline;
   private final long timeoutNanos;
   private int targetIndex;

   public StaggeredRequest(ResponseCollector<Address, T> responseCollector, long requestId, RequestRepository repository,
                           Collection<Address> targets, Address excludedTarget, JGroupsMetricsManager metricsManager,
                           TimeService timeService, ScheduledExecutorService timeoutExecutor, StaggeredSender sender,
                           long timeout, TimeUnit unit) {
      super(responseCollector, requestId, repository, targets, excludedTarget, metricsManager);

      this.sender = sender;
      this.timeService = timeService;
      this.timeoutExecutor = timeoutExecutor;
      this.timeoutNanos = unit.toNanos(timeout);
      this.deadline = timeService.expectedEndTime(timeout, unit);
   }

   @FunctionalInterface
   public interface StaggeredSender {
      void send(Address destination, long requestId);
   }

   @Override
   public void setTimeout(ScheduledExecutorService timeoutExecutor, long timeout, TimeUnit unit) {
      throw new UnsupportedOperationException("Timeout can only be set with sendFirstMessage!");
   }

   @Override
   protected void actualOnResponse(Address sender, Response response) {
      super.actualOnResponse(sender, response);
      sendNextMessage();
   }

   @Override
   protected void actualOnTimeout() {
      // Don't call super.onTimeout() if it's just a stagger timeout
      if (targetIndex >= getTargetsSize()) {
         super.actualOnTimeout();
      } else {
         sendNextMessage();
      }
   }

   void sendNextMessage() {
      try {
         RequestTracker target = null;
         boolean isFinalTarget;
         // Need synchronization because sendNextMessage can be called both directly and from addResponse()
         synchronized (responseCollector) {
            if (isDone() || targetIndex >= getTargetsSize()) {
               return;
            }

            // Skip over targets that are no longer in the cluster view
            while (target == null && targetIndex < getTargetsSize()) {
               target = getTarget(targetIndex++);
            }

            if (target == null) {
               // The final targets were removed because they have left the cluster,
               // but the request is not yet complete because we're still waiting for a response
               // from one of the other targets (i.e. we are being called from onTimeout).
               // We don't need to send another message, just wait for the real timeout to expire.
               long delayNanos = timeService.remainingTime(deadline, TimeUnit.NANOSECONDS);
               super.setTimeout(timeoutExecutor, delayNanos, TimeUnit.NANOSECONDS);
               return;
            }

            isFinalTarget = targetIndex >= getTargetsSize();
         }

         // Sending may block in flow-control or even in TCP, so we must do it outside the critical section
         target.resetSendTime();
         sender.send(target.destination(), requestId);

         // Scheduling the timeout task may also block
         // If this is the last target, set the request timeout at the deadline
         // Otherwise, schedule a timeout task to send a staggered request to the next target
         long delayNanos = timeService.remainingTime(deadline, TimeUnit.NANOSECONDS);
         if (!isFinalTarget) {
            delayNanos = delayNanos / 10 / getTargetsSize();
         }
         // On slow systems the scheduled executor may fire the stagger callback late,
         // after the original deadline has already passed. Enforce a minimum delay so
         // that every target we just sent a message to gets at least some time to respond.
         long minDelayNanos = timeoutNanos / 10 / getTargetsSize();
         delayNanos = Math.max(delayNanos, minDelayNanos);
         super.setTimeout(timeoutExecutor, delayNanos, TimeUnit.NANOSECONDS);
      } catch (Exception e) {
         completeExceptionally(e);
      }
   }
}
