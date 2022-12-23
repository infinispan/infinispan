package org.infinispan.remoting.inboundhandler;

import static org.infinispan.test.TestingUtil.wrapGlobalComponent;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.xsite.XSiteReplicateCommand;

@Scope(Scopes.GLOBAL)
public class ControllingInboundInvocationHandler extends BaseControllingHandler implements InboundInvocationHandler {
   private final InboundInvocationHandler delegate;

   public static ControllingInboundInvocationHandler replace(EmbeddedCacheManager manager) {
      return wrapGlobalComponent(manager, InboundInvocationHandler.class,
            iih -> {
               if (iih instanceof ControllingInboundInvocationHandler) {
                  return (ControllingInboundInvocationHandler) iih;
               }
               return new ControllingInboundInvocationHandler(iih, manager.getAddress());
            }, true);
   }

   private ControllingInboundInvocationHandler(InboundInvocationHandler delegate, Address address) {
      super(address);
      this.delegate = delegate;
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command,
                                 Reply reply, DeliverOrder order) {
      countCommand(command);
      blockIfNeeded(command, () -> delegate.handleFromCluster(origin, command, reply, order));
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteReplicateCommand<?> command,
                                    Reply reply, DeliverOrder order) {
      countCommand(command);
      blockIfNeeded(command, () -> delegate.handleFromRemoteSite(origin, command, reply, order));
   }

}
