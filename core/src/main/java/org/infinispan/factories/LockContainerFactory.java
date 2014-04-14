package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.OwnableReentrantStripedLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.concurrent.locks.containers.ReentrantStripedLockContainer;

/**
 * Factory class that creates instances of {@link org.infinispan.util.concurrent.locks.containers.LockContainer}.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@DefaultFactoryFor(classes = LockContainer.class)
public class LockContainerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public <T> T construct(Class<T> componentType) {
      boolean notTransactional = !configuration.transaction().transactionMode().isTransactional();
      LockContainer<?> lockContainer = configuration.locking().useLockStriping() ?
            notTransactional ? new ReentrantStripedLockContainer(configuration.locking().concurrencyLevel(),
                                                                 configuration.dataContainer().keyEquivalence())
                  : new OwnableReentrantStripedLockContainer(configuration.locking().concurrencyLevel(),
                                                             configuration.dataContainer().keyEquivalence()) :
            notTransactional ? new ReentrantPerEntryLockContainer(configuration.locking().concurrencyLevel(),
                                                                  configuration.dataContainer().keyEquivalence())
                  : new OwnableReentrantPerEntryLockContainer(configuration.locking().concurrencyLevel(),
                                                              configuration.dataContainer().keyEquivalence());
      return (T) lockContainer;
   }
}
