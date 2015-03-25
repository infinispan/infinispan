package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;

@DefaultFactoryFor(classes = CacheStoreFactoryRegistry.class)
public class CacheStoreFactoryRegistryFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      return (T) new CacheStoreFactoryRegistry();
   }
}
