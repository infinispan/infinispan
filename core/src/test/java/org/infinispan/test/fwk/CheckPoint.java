package org.infinispan.test.fwk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.test.TestingUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Behaves more or less like a map of {@link java.util.concurrent.Semaphore}s.
 *
 * One thread will wait for an event via {@code await(...)} or {@code awaitStrict(...)}, and one or more
 * other threads will trigger the event via {@code trigger(...)} or {@code triggerForever(...)}.
 *
 * @author Dan Berindei
 * @since 5.3
 */
public class CheckPoint {
   private static final Log log = LogFactory.getLog(CheckPoint.class);
   public static final int INFINITE = 999999999;

   private final String id;
   private final Lock lock = new ReentrantLock();
   private final Condition unblockCondition = lock.newCondition();
   private final Map<String, EventStatus> events = new HashMap<>();

   public CheckPoint() {
      this.id = "";
   }

   public CheckPoint(String name) {
      this.id = "[" + name + "] ";
   }

   public void awaitStrict(String event, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      awaitStrict(event, 1, timeout, unit);
   }

   public CompletionStage<Void> awaitStrictAsync(String event, long timeout, TimeUnit unit, Executor executor) {
      return CompletableFuture.runAsync(() -> {
         try {
            awaitStrict(event, 1, timeout, unit);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         } catch (TimeoutException e) {
            CompletableFutures.rethrowExceptionIfPresent(e);
         }
      }, executor);
   }

   private boolean await(String event, long timeout, TimeUnit unit) throws InterruptedException {
      return await(event, 1, timeout, unit);
   }

   public void awaitStrict(String event, int count, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      if (!await(event, count, timeout, unit)) {
         throw new TimeoutException(id + "Timed out waiting for event " + event);
      }
   }

   private boolean await(String event, int count, long timeout, TimeUnit unit) throws InterruptedException {
      log.tracef("%sWaiting for event %s * %d", id, event, count);
      lock.lock();
      try {
         EventStatus status = events.computeIfAbsent(event, k -> new EventStatus());
         long waitNanos = unit.toNanos(timeout);
         while (waitNanos > 0) {
            if (status.available >= count) {
               status.available -= count;
               break;
            }
            waitNanos = unblockCondition.awaitNanos(waitNanos);
         }

         if (waitNanos <= 0) {
            log.errorf("%sTimed out waiting for event %s * %d (available = %d, total = %d)",
                       id, event, count, status.available, status.total);
            // let the triggering thread know that we timed out
            status.available = -1;
            return false;
         }

         log.tracef("%sReceived event %s * %d (available = %d, total = %d)", id, event, count,
                    status.available, status.total);
         return true;
      } finally {
         lock.unlock();
      }
   }

   public String peek(long timeout, TimeUnit unit, String... expectedEvents) throws InterruptedException {
      log.tracef("%sWaiting for any one of events %s", id, Arrays.toString(expectedEvents));
      String found = null;
      lock.lock();
      try {
         long waitNanos = unit.toNanos(timeout);
         while (waitNanos > 0) {
            for (String event : expectedEvents) {
               EventStatus status = events.get(event);
               if (status != null && status.available >= 1) {
                  found = event;
                  break;
               }
            }
            if (found != null)
               break;

            waitNanos = unblockCondition.awaitNanos(waitNanos);
         }

         if (waitNanos <= 0) {
            log.tracef("%sPeek did not receive any of %s", id, Arrays.toString(expectedEvents));
            return null;
         }

         EventStatus status = events.get(found);
         log.tracef("%sReceived event %s (available = %d, total = %d)", id, found, status.available, status.total);
         return found;
      } finally {
         lock.unlock();
      }
   }

   public CompletableFuture<Void> future(String event, long timeout, TimeUnit unit, Executor executor) {
      return future(event, 1, timeout, unit, executor);
   }

   public CompletableFuture<Void> future(String event, int count, long timeout, TimeUnit unit, Executor executor) {
      return TestingUtil.orTimeout(future0(event, count), timeout, unit, executor)
                        .thenRunAsync(() -> log.tracef("Received event %s * %d", event, count), executor);
   }

   public CompletableFuture<Void> future0(String event, int count) {
      log.tracef("%sWaiting for event %s * %d", id, event, count);
      lock.lock();
      try {
         EventStatus status = events.get(event);
         if (status == null) {
            status = new EventStatus();
            events.put(event, status);
         }
         if (status.available >= count) {
            status.available -= count;
            return CompletableFutures.completedNull();
         }
         if (status.requests == null) {
            status.requests = new ArrayList<>();
         }
         CompletableFuture<Void> f = new CompletableFuture<>();
         status.requests.add(new Request(f, count));
         return f;
      } finally {
         lock.unlock();
      }
   }

   public void trigger(String event) {
      trigger(event, 1);
   }

   public void triggerForever(String event) {
      trigger(event, INFINITE);
   }

   public void trigger(String event, int count) {
      lock.lock();
      try {
         EventStatus status = events.get(event);
         if (status == null) {
            status = new EventStatus();
            events.put(event, status);
         } else if (status.available < 0) {
            throw new IllegalStateException(id + "Thread already timed out waiting for event " + event);
         }

         // If triggerForever is called more than once, it will cause an overflow and the waiters will fail.
         status.available = count != INFINITE ? status.available + count : INFINITE;
         status.total = count != INFINITE ? status.total + count : INFINITE;
         log.tracef("%sTriggering event %s * %d (available = %d, total = %d)", id, event, count,
                    status.available, status.total);
         unblockCondition.signalAll();
         if (status.requests != null) {
            if (count == INFINITE) {
               status.requests.forEach(request -> request.future.complete(null));
            } else {
               Iterator<Request> iterator = status.requests.iterator();
               while (status.available > 0 && iterator.hasNext()){
                  Request request = iterator.next();
                  if (request.count <= status.available) {
                     request.future.complete(null);
                     status.available -= request.count;
                     iterator.remove();
                  }
               }
            }
         }
      } finally {
         lock.unlock();
      }
   }

   @Override
   public String toString() {
      return "CheckPoint(" + id + ")" + events;
   }

   private static class EventStatus {
      int available;
      int total;
      public ArrayList<Request> requests;

      @Override
      public String toString() {
         return "" + available + "/" + total + ", requests=" + requests;
      }
   }

   private static class Request {
      final CompletableFuture<Void> future;
      final int count;

      private Request(CompletableFuture<Void> future, int count) {
         this.future = future;
         this.count = count;
      }

      @Override
      public String toString() {
         return "(" + count + ")";
      }
   }
}
