package org.infinispan.factories;

import static org.infinispan.util.logging.Log.CONTAINER;

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
   public Object construct(String componentName) {
      if (!configuration.clustering().cacheMode().isClustered())
         return null;

      if (componentName.equals(StateTransferManager.class.getName())) {
         return new StateTransferManagerImpl();
      } else if (componentName.equals(StateProvider.class.getName())) {
         return new StateProviderImpl();
      } else if (componentName.equals(StateConsumer.class.getName())) {
         return new StateConsumerImpl();
      } else if (componentName.equals(StateReceiver.class.getName())) {
         return new StateReceiverImpl<>();
      } else if (componentName.equals(ConflictManager.class.getName()) || componentName.equals(InternalConflictManager.class.getName())) {
         return new DefaultConflictManager<>();
      }

      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
