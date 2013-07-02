package org.infinispan.factories;

import org.infinispan.container.EntryFactory;
import org.infinispan.container.EntryFactoryImpl;
import org.infinispan.container.IncrementalVersionableEntryFactoryImpl;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;

@DefaultFactoryFor(classes = {EntryFactory.class, InternalEntryFactory.class})
public class EntryMetaFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {

      // If we are repeatable-read and have write skew checking enabled and are clustered, lets create an appropriate EntryFactory.
      boolean useVersioning = configuration.clustering().cacheMode().isClustered() &&
            configuration.transaction().transactionMode().isTransactional() &&
            configuration.locking().isolationLevel() == IsolationLevel.REPEATABLE_READ &&
            configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC;

      if (componentType.equals(EntryFactory.class)) {
         if (useVersioning)
            return (T) new IncrementalVersionableEntryFactoryImpl();
         else
            return (T) new EntryFactoryImpl();
      } else {
         return (T) new InternalEntryFactoryImpl();
      }
   }
}
