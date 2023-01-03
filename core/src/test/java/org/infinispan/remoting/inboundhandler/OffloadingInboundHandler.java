package org.infinispan.remoting.inboundhandler;

import java.util.Objects;
import java.util.concurrent.Executor;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.XSiteReplicateCommand;

public class OffloadingInboundHandler implements InboundInvocationHandler {

   private final InboundInvocationHandler delegate;
   private final Executor executor;

   public static void wrap(EmbeddedCacheManager cacheManager, Executor executor) {
      Objects.requireNonNull(cacheManager);
      Objects.requireNonNull(executor);
      TestingUtil.wrapGlobalComponent(cacheManager, InboundInvocationHandler.class, delegate -> new OffloadingInboundHandler(delegate, executor), true);
   }

   private OffloadingInboundHandler(InboundInvocationHandler delegate, Executor executor) {
      this.delegate = delegate;
      this.executor = executor;
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      executor.execute(() -> delegate.handleFromCluster(origin, command, reply, order));
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteReplicateCommand<?> command, Reply reply, DeliverOrder order) {
      executor.execute(() -> delegate.handleFromRemoteSite(origin, command, reply, order));
   }
}
