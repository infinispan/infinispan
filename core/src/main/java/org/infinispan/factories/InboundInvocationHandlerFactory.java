package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.inboundhandler.NonTotalOrderPerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.NonTotalOrderTxPerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.TotalOrderTxPerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.TrianglePerCacheInboundInvocationHandler;

/**
 * Factory class that creates instances of {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler}.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@DefaultFactoryFor(classes = PerCacheInboundInvocationHandler.class)
public class InboundInvocationHandlerFactory extends AbstractNamedCacheComponentFactory implements
      AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.clustering().cacheMode().isClustered()) {
         return null;
      } else if (configuration.transaction().transactionMode().isTransactional()) {
         return configuration.transaction().transactionProtocol().isTotalOrder() ?
               componentType.cast(new TotalOrderTxPerCacheInboundInvocationHandler()) :
               componentType.cast(new NonTotalOrderTxPerCacheInboundInvocationHandler());
      } else {
         return configuration.clustering().cacheMode().isDistributed() ?
               componentType.cast(new TrianglePerCacheInboundInvocationHandler()) :
               componentType.cast(new NonTotalOrderPerCacheInboundInvocationHandler());
      }
   }
}
