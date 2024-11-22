package org.infinispan.remoting.transport.jgroups;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.impl.MultiTargetRequest;
import org.infinispan.remoting.transport.impl.RequestRepository;

import net.jcip.annotations.GuardedBy;

/**
 * @author Dan Berindei
 * @since 9.1
 */
public class StaggeredRequest<T> extends MultiTargetRequest<T> {
   private final ReplicableCommand command;
   private final DeliverOrder deliverOrder;
   private final JGroupsTransport transport;

   @GuardedBy("responseCollector")
   private final long deadline;
   @GuardedBy("responseCollector")
   private int targetIndex;
   private final Lock lock;

   StaggeredRequest(ResponseCollector<T> responseCollector, long requestId, RequestRepository repository,
                    Collection<Address> targets, Address excludedTarget, ReplicableCommand command,
                    DeliverOrder deliverOrder, long timeout, TimeUnit unit, JGroupsTransport transport) {
      super(responseCollector, requestId, repository, targets, excludedTarget, transport.metricsManager);

      this.command = command;
      this.deliverOrder = deliverOrder;
      this.transport = transport;
      this.deadline = transport.timeService.expectedEndTime(timeout, unit);
      this.lock = new ReentrantLock();
   }

   @Override
   public void setTimeout(ScheduledExecutorService timeoutExecutor, long timeout, TimeUnit unit) {
      throw new UnsupportedOperationException("Timeout can only be set with sendFirstMessage!");
   }

   @Override
   public void onResponse(Address sender, Response response) {
      lock.lock();
      try {
         super.onResponse(sender, response);
         sendNextMessage();
      } finally {
         lock.unlock();
      }
   }

   @Override
   protected void onTimeout() {
      // Don't call super.onTimeout() if it's just a stagger timeout
      boolean isFinalTimeout;
      synchronized (responseCollector) {
         isFinalTimeout = targetIndex >= getTargetsSize();
      }

      if (isFinalTimeout) {
         super.onTimeout();
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
         transport.sendCommand(target.destination(), command, requestId, deliverOrder, true, false);

         // Scheduling the timeout task may also block
         // If this is the last target, set the request timeout at the deadline
         // Otherwise, schedule a timeout task to send a staggered request to the next target
         long delayNanos = transport.getTimeService().remainingTime(deadline, TimeUnit.NANOSECONDS);
         if (!isFinalTarget) {
            delayNanos = delayNanos / 10 / getTargetsSize();
         }
         super.setTimeout(transport.getTimeoutExecutor(), delayNanos, TimeUnit.NANOSECONDS);
      } catch (Exception e) {
         completeExceptionally(e);
      }
   }
}
