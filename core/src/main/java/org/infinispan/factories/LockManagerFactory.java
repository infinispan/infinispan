package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockManagerImpl;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.impl.DefaultPendingLockManager;
import org.infinispan.util.concurrent.locks.impl.NoOpPendingLockManager;

/**
 * Factory class that creates instances of {@link LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {LockManager.class, PendingLockManager.class} )
public class LockManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (PendingLockManager.class.equals(componentType)) {
         return componentType.cast(configuration.clustering().cacheMode().isClustered() ?
                                         new DefaultPendingLockManager() :
                                         NoOpPendingLockManager.getInstance());
      } else if (LockManager.class.equals(componentType)) {
         return componentType.cast(configuration.deadlockDetection().enabled() ?
                                         new DeadlockDetectingLockManager() :
                                         new LockManagerImpl());
      }
      throw new IllegalArgumentException("Unexpected component type " + componentType + ".");
   }
}
