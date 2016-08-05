package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.scattered.impl.ScatteredStateConsumerImpl;
import org.infinispan.scattered.impl.ScatteredStateProviderImpl;
import org.infinispan.statetransfer.*;

/**
 * Constructs {@link org.infinispan.statetransfer.StateTransferManager},
 * {@link org.infinispan.statetransfer.StateConsumer}
 * and {@link org.infinispan.statetransfer.StateProvider} instances.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @author anistor@redhat.com
 * @since 4.0
 */
@DefaultFactoryFor(classes = {StateTransferManager.class, StateConsumer.class, StateProvider.class})
public class StateTransferComponentFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.clustering().cacheMode().isClustered())
         return null;

      if (componentType.equals(StateTransferManager.class)) {
         return componentType.cast(new StateTransferManagerImpl());
      } else if (componentType.equals(StateProvider.class)) {
         if (configuration.clustering().cacheMode().isScattered()) {
            return componentType.cast(new ScatteredStateProviderImpl());
         } else {
            return componentType.cast(new StateProviderImpl());
         }
      } else if (componentType.equals(StateConsumer.class)) {
         if (configuration.clustering().cacheMode().isScattered()) {
            return componentType.cast(new ScatteredStateConsumerImpl());
         } else {
            return componentType.cast(new StateConsumerImpl());
         }
      }

      throw new CacheConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
