package org.infinispan.util.concurrent.locks.deadlock;

import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.DeadlockDetection;

@DefaultFactoryFor(classes = {DeadlockDetection.class})
public class DistributedDeadlockDetectionFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (configuration.transaction().deadlockDetectionEnabled() && isPessimistic())
         return new DistributedDeadlockDetection();

      return DisabledDeadlockDetection.getInstance();
   }

   private boolean isPessimistic() {
      return configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
   }
}
