package org.infinispan.server.core.transport;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.CacheException;
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

/**
 * This event loop group prevents any tasks submitted via the {@link java.util.concurrent.ExecutorService#execute(Runnable)}},
 * {@link java.util.concurrent.ExecutorService#submit(Runnable)}, {@link java.util.concurrent.ExecutorService#submit(Callable)},
 * {@link java.util.concurrent.ExecutorService#submit(Runnable, Object)} API methods from
 * being ran on the same event loop that submitted it. This is to prevent issues where some operations block on
 * the result of another task (e.g. Cache creation).
 */
@Scope(Scopes.GLOBAL)
public class NonRecursiveEventLoopGroup extends DelegatingEventLoopGroup {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final MultithreadEventLoopGroup eventLoopGroup;

   public NonRecursiveEventLoopGroup(MultithreadEventLoopGroup eventLoopGroup) {
      int executors = eventLoopGroup.executorCount();
      if (executors < 2) {
         throw new IllegalArgumentException("Provided multi threaded event loop group must have at least 2 executors, only has " + executors);
      }
      this.eventLoopGroup = eventLoopGroup;
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


   private EventExecutor getExecutorNotInEventLoop() {
      while (true) {
         EventExecutor eventExecutor = eventLoopGroup.next();
         // We don't want to submit a task to our current thread as some callers may block waiting for it to complete.
         if (eventExecutor.inEventLoop()) {
            log.tracef("Skipped submitting task to %s as it is the current event loop - trying another", eventExecutor);
            continue;
         }

         return eventExecutor;
      }
   }

   @Stop
   public void shutdownGracefullyAndWait() {
      try {
         shutdownGracefully().get(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         log.debug("Interrupted while waiting for event loop group to shut down");
         Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
         throw new CacheException(e.getCause());
      } catch (TimeoutException e) {
         throw new org.infinispan.util.concurrent.TimeoutException("Timed out waiting for event loop group to shutdown", e);
      }
   }
}
