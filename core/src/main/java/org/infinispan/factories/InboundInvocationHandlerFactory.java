package org.infinispan.factories;

import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.Configurations;
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
   public Object construct(String componentName) {
      if (!configuration.clustering().cacheMode().isClustered()) {
         return null;
      } else if (configuration.transaction().transactionMode().isTransactional()) {
         return configuration.transaction().transactionProtocol().isTotalOrder() ?
                new TotalOrderTxPerCacheInboundInvocationHandler() :
                new NonTotalOrderTxPerCacheInboundInvocationHandler();
      } else {
         if (configuration.clustering().cacheMode().isDistributed() && Configurations.isEmbeddedMode(globalConfiguration)
               || configuration.clustering().cacheMode().isScattered() && configuration.clustering().biasAcquisition() != BiasAcquisition.NEVER) {
            return new TrianglePerCacheInboundInvocationHandler();
         } else {
            return new NonTotalOrderPerCacheInboundInvocationHandler();
         }
      }
   }
}
