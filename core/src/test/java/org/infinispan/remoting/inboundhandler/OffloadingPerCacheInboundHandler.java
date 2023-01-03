package org.infinispan.remoting.inboundhandler;

import java.util.Objects;
import java.util.concurrent.Executor;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.test.TestingUtil;

public class OffloadingPerCacheInboundHandler implements PerCacheInboundInvocationHandler {

   private final PerCacheInboundInvocationHandler delegate;
   private final Executor executor;

   public static void wrap(Cache<?, ?> cache, Executor executor) {
      Objects.requireNonNull(cache);
      Objects.requireNonNull(executor);
      TestingUtil.wrapInboundInvocationHandler(cache, delegate -> new OffloadingPerCacheInboundHandler(delegate, executor));
   }

   private OffloadingPerCacheInboundHandler(PerCacheInboundInvocationHandler delegate, Executor executor) {
      this.delegate = delegate;
      this.executor = executor;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      executor.execute(() -> delegate.handle(command, reply, order));
   }

   @Override
   public void setFirstTopologyAsMember(int firstTopologyAsMember) {
      delegate.setFirstTopologyAsMember(firstTopologyAsMember);
   }

   @Override
   public int getFirstTopologyAsMember() {
      return delegate.getFirstTopologyAsMember();
   }

   @Override
   public void checkForReadyTasks() {
      delegate.checkForReadyTasks();
   }
}
