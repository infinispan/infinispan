package org.infinispan.server.core.transport;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.utils.DelegatingEventLoopGroup;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.ThreadExecutorMap;

/**
 * This event loop group prevents any tasks submitted via {@link java.util.concurrent.ExecutorService} API from
 * being ran on the same event loop that submitted it. This is to prevent issues where some operations block on
 * the result of another task (e.g. Cache creation).
 */
@Scope(Scopes.GLOBAL)
public class NonRecursiveEventLoopGroup extends DelegatingEventLoopGroup {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final MultithreadEventLoopGroup eventLoopGroup;
   private final int loopAttempts;

   private static final Set<EventExecutor> reserveExecutors = ConcurrentHashMap.newKeySet();

   public NonRecursiveEventLoopGroup(MultithreadEventLoopGroup eventLoopGroup) {
      int executors = eventLoopGroup.executorCount();
      if (executors < 2) {
         throw new IllegalArgumentException("Provided multi threaded event loop group must have at least 2 executors, only has " + executors);
      }
      this.eventLoopGroup = eventLoopGroup;
      // Try to get an event loop a bit more than the number of executors
      this.loopAttempts = executors << 1;
   }

   @Override
   protected EventLoopGroup delegate() {
      return eventLoopGroup;
   }

   @Override
   public void execute(Runnable command) {
      getExecutorNotInEventLoop().execute(command);
   }

   @Override
   public Future<?> submit(Runnable task) {
      return getExecutorNotInEventLoop().submit(task);
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      return getExecutorNotInEventLoop().submit(task);
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      return getExecutorNotInEventLoop().submit(task, result);
   }

   /**
    * This reserves the current thread's executor so that no other executor may submit tasks to it including itself.
    * This should be used if the calling code will end up blocking when it shouldn't and is a sign of code that
    * should eventually be refactored but provides a way to share the netty event loop
    */
   public static void reserveCurrentThread() {
      EventExecutor thisExecutor = ThreadExecutorMap.currentExecutor();
      if (thisExecutor != null) {
         boolean reserved = reserveExecutors.add(thisExecutor);
         assert reserved;
      }
   }

   /**
    * No longer reserves the current thread and allows it to be submitted to. This should always be invoked when
    * the blocking in the invoking thread is complete as soon as possible.
    */
   public static void unreserveCurrentThread() {
      EventExecutor thisExecutor = ThreadExecutorMap.currentExecutor();
      if (thisExecutor != null) {
         boolean unreserved = reserveExecutors.remove(thisExecutor);
         assert unreserved;
      }
   }

   private EventExecutor getExecutorNotInEventLoop() {
      for (int i = 0; i < loopAttempts; ++i) {
         EventExecutor eventExecutor = eventLoopGroup.next();
         // We don't want to submit a task to our current thread as some callers may block waiting for it to complete.
         if (eventExecutor.inEventLoop()) {
            log.tracef("Skipped submitting task to %s as it is the current event loop - trying another", eventExecutor);
            continue;
         }
         // We also don't want to submit to a reserved executor as it is currently blocking waiting for another caller
         if (reserveExecutors.contains(eventExecutor)) {
            log.tracef("Skipped submitting task to %s as it is currently reserved - trying another", eventExecutor);
            continue;
         }

         return eventExecutor;
      }
      log.fatalf("No executor was found - replaying to trace issue - current reserveExecutors are %s", reserveExecutors);
      for (int i = 0; i < loopAttempts; ++i) {
         EventExecutor eventExecutor = eventLoopGroup.next();
         // We don't want to submit a task to our current thread as some callers may block waiting for it to complete.
         if (eventExecutor.inEventLoop()) {
            log.fatalf("Skipped submitting task to %s as it is the current event loop - trying another", eventExecutor);
            continue;
         }
         // We also don't want to submit to a reserved executor as it is currently blocking waiting for another caller
         if (reserveExecutors.contains(eventExecutor)) {
            log.fatalf("Skipped submitting task to %s as it is currently reserved - trying another", eventExecutor);
            continue;
         }

         log.fatalf("Found a matched executor %s, possibly concurrent reservation?", eventExecutor);
      }
      throw new IllegalStateException("Unable to find an executor not in the event loop or that wasn't reserved " +
            reserveExecutors + " - this is a bug!");
   }

   @Stop
   @Override
   public void shutdown() {
      super.shutdown();
   }
}
