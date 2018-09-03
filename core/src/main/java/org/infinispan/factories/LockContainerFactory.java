package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.infinispan.util.concurrent.locks.impl.StripedLockContainer;

/**
 * Factory class that creates instances of {@link LockContainer}.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@DefaultFactoryFor(classes = LockContainer.class)
public class LockContainerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public Object construct(String componentName) {
      return configuration.locking().useLockStriping() ?
             new StripedLockContainer(configuration.locking().concurrencyLevel()) :
             new PerKeyLockContainer();
   }
}
