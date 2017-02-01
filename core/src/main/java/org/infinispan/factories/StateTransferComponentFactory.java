package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.impl.DefaultConflictManager;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.conflict.impl.StateReceiver;
import org.infinispan.conflict.impl.StateReceiverImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateConsumerImpl;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.StateProviderImpl;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.statetransfer.StateTransferManagerImpl;

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
@DefaultFactoryFor(classes = {StateTransferManager.class, StateConsumer.class, StateProvider.class, StateReceiver.class,
      ConflictManager.class, InternalConflictManager.class})
public class StateTransferComponentFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.clustering().cacheMode().isClustered())
         return null;

      if (componentType.equals(StateTransferManager.class)) {
         return componentType.cast(new StateTransferManagerImpl());
      } else if (componentType.equals(StateProvider.class)) {
         return componentType.cast(new StateProviderImpl());
      } else if (componentType.equals(StateConsumer.class)) {
         return componentType.cast(new StateConsumerImpl());
      } else if (componentType.equals(StateReceiver.class)) {
         return componentType.cast(new StateReceiverImpl<>());
      } else if (componentType.isAssignableFrom(InternalConflictManager.class)) {
         return componentType.cast(new DefaultConflictManager<>());
      }

      throw new CacheConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
