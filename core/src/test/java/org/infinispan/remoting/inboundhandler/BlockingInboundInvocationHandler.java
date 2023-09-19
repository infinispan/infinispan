package org.infinispan.remoting.inboundhandler;

import static org.infinispan.test.TestingUtil.wrapGlobalComponent;

import java.util.function.Predicate;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.NotifierLatch;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.xsite.commands.remote.XSiteRequest;

@Scope(Scopes.GLOBAL)
public class BlockingInboundInvocationHandler implements InboundInvocationHandler {
   private final Address address;
   private final NotifierLatch latch;
   private final InboundInvocationHandler delegate;
   @Inject BlockingManager blockingManager;
   private volatile Predicate<ReplicableCommand> predicate;

   public static BlockingInboundInvocationHandler replace(EmbeddedCacheManager manager) {
      return wrapGlobalComponent(manager, InboundInvocationHandler.class,
                                 iih -> new BlockingInboundInvocationHandler(iih, manager.getAddress()), true);
   }

   public BlockingInboundInvocationHandler(InboundInvocationHandler delegate, Address address) {
      this.delegate = delegate;
      this.address = address;
      latch = new NotifierLatch(toString());
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command,
                                 Reply reply, DeliverOrder order) {
      Predicate<ReplicableCommand> predicate = this.predicate;
      if (predicate != null && predicate.test(command)) {
         blockingManager.runBlocking(() -> {
            latch.blockIfNeeded();
            delegate.handleFromCluster(origin, command, reply, order);
         }, "blocking-inbound-handler");
         return;
      }
      delegate.handleFromCluster(origin, command, reply, order);
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteRequest<?> command,
                                    Reply reply, DeliverOrder order) {
      delegate.handleFromRemoteSite(origin, command, reply, order);
   }

   public NotifierLatch latch() {
      return latch;
   }

   public <T extends ReplicableCommand> void blockBefore(Class<T> commandClass, Predicate<T> predicate) {
      this.predicate = c -> commandClass.isInstance(c) && predicate.test(commandClass.cast(c));
      latch.startBlocking();
   }

   public void blockBefore(Class<? extends ReplicableCommand> commandClass) {
      this.predicate = commandClass::isInstance;
      latch.startBlocking();
   }

   public void stopBlocking() {
      latch.stopBlocking();
   }

   @Override
   public String toString() {
      return "BlockingInboundInvocationHandler@" + address;
   }
}
