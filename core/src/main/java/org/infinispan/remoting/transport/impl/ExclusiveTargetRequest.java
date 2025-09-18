package org.infinispan.remoting.transport.impl;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.AbstractRequest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Request implementation that waits for responses from multiple target nodes.
 * <p>
 * This Request handles concurrent calls to {@link #onResponse(Address, Response)},
 * {@link #onNewView(Set)} and {@link #onTimeout()} by using a very small synchronized block to guarantee only a single
 * thread is performing any these actual operations at a time. This is done by updating the instance variable {@link #handling}
 * to <b>true</b> for the first caller to enter one of these methods. The thread that updates it to true will perform their
 * operation immediately. However, if another concurrent call enters any of these methods and {@link #handling} is already
 * <b>true</b> this thread will instead initialize the {@link #queue} if necessary and add its operation as a Runnable to it.
 * The caller that updated {@link #handling} to <b>true</b> will after performing its operation check under lock if the
 * {@link #queue} has been updated and if so run any of those operations, thus guaranteeing thread safety.
 * <p>
 * Note that {@link #onNewView(Set)} return value is only valid if invoked before submitting this request. This is due
 * to the update being possibly enqueued, and thus we cannot guarantee its correctness.
 * @author William Burns
 */
public abstract class ExclusiveTargetRequest<T> extends AbstractRequest<Address, T> {
   @GuardedBy("this")
   private Queue<Runnable> queue;
   @GuardedBy("this")
   private boolean handling;

   public ExclusiveTargetRequest(ResponseCollector<Address, T> responseCollector, long requestId, RequestRepository repository) {
      super(requestId, responseCollector, repository);
   }

   @Override
   public final void onResponse(Address sender, Response response) {
      if (isDone()) {
         return;
      }
      synchronized (this) {
         if (isDone()) {
            return;
         }
         if (handling) {
            enqueueOperation(() -> actualOnResponse(sender, response));
            return;
         }
         handling = true;
      }
      actualOnResponse(sender, response);
      handleLoop();
   }

   @Override
   public final boolean onNewView(Set<Address> members) {
      if (isDone()) {
         return false;
      }
      boolean response;
      synchronized (this) {
         if (isDone()) {
            return false;
         }
         if (handling) {
            enqueueOperation(() -> actualOnView(members));
            return false;
         }
         handling = true;
      }
      response = actualOnView(members);
      handleLoop();
      return response;
   }

   @Override
   protected final void onTimeout() {
      if (isDone()) {
         return;
      }
      synchronized (this) {
         if (isDone()) {
            return;
         }
         if (handling) {
            enqueueOperation(this::actualOnTimeout);
            return;
         }
         handling = true;
      }
      actualOnTimeout();
      handleLoop();
   }

   @GuardedBy("this")
   private void enqueueOperation(Runnable runnable) {
      Queue<Runnable> queue = this.queue;
      if (queue == null) {
         queue = new ArrayDeque<>(2);
         this.queue = queue;
      }
      queue.add(runnable);
   }

   /**
    * This method can only be invoked after updating {@link #handling} from <b>false</b> to <b>true</b> while under
    * the monitor for <b>this</b>.
    */
   private void handleLoop() {
      //noinspection FieldAccessNotGuarded
      assert handling;
      // Note each loop will use the synchronized block to pull the queue to a local reference and set the
      // instance variable back to null allowing for additional concurrent updates to create/insert into a new
      // queue to be processed on a subsequent loop
      while (!isDone()) {
         Queue<Runnable> queue;
         synchronized (this) {
            queue = this.queue;
            if (queue == null) {
               handling = false;
               return;
            }
            this.queue = null;
         }
         queue.forEach(Runnable::run);
      }
   }

   protected abstract void actualOnResponse(Address sender, Response response);

   protected abstract boolean actualOnView(Set<Address> members);

   protected abstract void actualOnTimeout();
}
