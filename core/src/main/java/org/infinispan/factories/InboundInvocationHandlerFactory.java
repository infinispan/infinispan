package org.infinispan.factories;

import org.infinispan.configuration.cache.Configurations;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.inboundhandler.NonTxPerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.TrianglePerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.TxPerCacheInboundInvocationHandler;

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
         return new TxPerCacheInboundInvocationHandler();
      } else {
         if (configuration.clustering().cacheMode().isDistributed() && Configurations.isEmbeddedMode(globalConfiguration)) {
            return new TrianglePerCacheInboundInvocationHandler();
         } else {
            return new NonTxPerCacheInboundInvocationHandler();
         }
      }
   }
}
