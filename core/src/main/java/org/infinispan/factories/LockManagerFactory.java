package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;
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
   public Object construct(String componentName) {
      if (PendingLockManager.class.getName().equals(componentName)) {
         return configuration.clustering().cacheMode().isClustered() ?
                new DefaultPendingLockManager() :
                NoOpPendingLockManager.getInstance();
      } else if (LockManager.class.getName().equals(componentName)) {
         return new DefaultLockManager();
      }
      throw log.factoryCannotConstructComponent(componentName);
   }
}
