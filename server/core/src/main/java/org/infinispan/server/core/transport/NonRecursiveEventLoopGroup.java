package org.infinispan.server.core.transport;

import java.util.concurrent.Callable;

import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.server.core.utils.DelegatingEventLoopGroup;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;

/**
 * This event loop group prevents any tasks submitted via {@link java.util.concurrent.ExecutorService} API from
 * being ran on the same event loop that submitted it. This is to prevent issues where some operations block on
 * the result of another task (e.g. Cache creation).
 */
@Scope(Scopes.GLOBAL)
public class NonRecursiveEventLoopGroup extends DelegatingEventLoopGroup {
   private final EventLoopGroup eventLoopGroup;

   public NonRecursiveEventLoopGroup(EventLoopGroup eventLoopGroup) {
      // This ensures there is at least 2 event loops - we can't support 1
      eventLoopGroup.iterator().next().next();
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
      for (EventExecutor eventExecutor : eventLoopGroup) {
         if (!eventExecutor.inEventLoop()) {
            return eventExecutor;
         }
      }
      throw new IllegalStateException("Unable to find an executor not in the event loop - this is a bug!");
   }

   @Stop
   @Override
   public Future<?> shutdownGracefully() {
      return super.shutdownGracefully();
   }
}
