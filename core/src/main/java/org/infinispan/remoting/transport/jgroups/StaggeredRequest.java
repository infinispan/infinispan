package org.infinispan.remoting.transport.jgroups;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
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
   private final ReplicableCommand command;
   private final DeliverOrder deliverOrder;
   private final JGroupsTransport transport;

   private final long deadline;
   private final long timeoutNanos;
   private int targetIndex;

   StaggeredRequest(ResponseCollector<Address, T> responseCollector, long requestId, RequestRepository repository,
                    Collection<Address> targets, Address excludedTarget, ReplicableCommand command,
                    DeliverOrder deliverOrder, long timeout, TimeUnit unit, JGroupsTransport transport) {
      super(responseCollector, requestId, repository, targets, excludedTarget, transport.metricsManager);

      this.command = command;
      this.deliverOrder = deliverOrder;
      this.transport = transport;
      this.timeoutNanos = unit.toNanos(timeout);
      this.deadline = transport.timeService.expectedEndTime(timeout, unit);
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
               long delayNanos = transport.getTimeService().remainingTime(deadline, TimeUnit.NANOSECONDS);
               super.setTimeout(transport.getTimeoutExecutor(), delayNanos, TimeUnit.NANOSECONDS);
               return;
            }

            isFinalTarget = targetIndex >= getTargetsSize();
         }

         // Sending may block in flow-control or even in TCP, so we must do it outside the critical section
         target.resetSendTime();
         transport.sendCommandCheckingView(target.destination(), command, requestId, deliverOrder);

         // Scheduling the timeout task may also block
         // If this is the last target, set the request timeout at the deadline
         // Otherwise, schedule a timeout task to send a staggered request to the next target
         long delayNanos = transport.getTimeService().remainingTime(deadline, TimeUnit.NANOSECONDS);
         if (!isFinalTarget) {
            delayNanos = delayNanos / 10 / getTargetsSize();
         }
         // On slow systems the scheduled executor may fire the stagger callback late,
         // after the original deadline has already passed. Enforce a minimum delay so
         // that every target we just sent a message to gets at least some time to respond.
         long minDelayNanos = timeoutNanos / 10 / getTargetsSize();
         delayNanos = Math.max(delayNanos, minDelayNanos);
         super.setTimeout(transport.getTimeoutExecutor(), delayNanos, TimeUnit.NANOSECONDS);
      } catch (Exception e) {
         completeExceptionally(e);
      }
   }
}
