package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.inboundhandler.NonTotalOrderPerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.TotalOrderTxPerCacheInboundInvocationHandler;

/**
 * Factory class that creates instances of {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler}.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@DefaultFactoryFor(classes = PerCacheInboundInvocationHandler.class)
public class InboundInvocationHandlerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.clustering().cacheMode().isClustered()) {
         return null;
      } else if (configuration.transaction().transactionMode().isTransactional() &&
            configuration.transaction().transactionProtocol().isTotalOrder()) {
         //noinspection unchecked
         return (T) new TotalOrderTxPerCacheInboundInvocationHandler();
      } else {
         //noinspection unchecked
         return (T) new NonTotalOrderPerCacheInboundInvocationHandler();
      }
   }
}
