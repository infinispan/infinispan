package org.infinispan.remoting.inboundhandler;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

@Scope(Scopes.NAMED_CACHE)
public class ControllingPerCacheInboundInvocationHandler extends BaseControllingHandler implements PerCacheInboundInvocationHandler {

   private final PerCacheInboundInvocationHandler delegate;

   public static ControllingPerCacheInboundInvocationHandler replace(Cache<?, ?> cache) {
      return wrapInboundInvocationHandler(cache,
            iih -> {
               if (iih instanceof ControllingPerCacheInboundInvocationHandler) {
                  return (ControllingPerCacheInboundInvocationHandler) iih;
               }
               return new ControllingPerCacheInboundInvocationHandler(iih, cache.getCacheManager().getAddress());
            });
   }

   private ControllingPerCacheInboundInvocationHandler(PerCacheInboundInvocationHandler delegate, Address address) {
      super(address);
      this.delegate = delegate;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      countCommand(command);
      blockIfNeeded(command, () -> delegate.handle(command, reply, order));
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
