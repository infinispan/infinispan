package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockManagerImpl;

/**
 * Factory class that creates instances of {@link LockManager}.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = LockManager.class)
public class LockManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.deadlockDetection().enabled()) {
         return (T) new DeadlockDetectingLockManager();
      } else {
         return (T) new LockManagerImpl();
      }
   }
}
