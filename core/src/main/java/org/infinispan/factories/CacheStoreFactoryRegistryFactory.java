package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = CacheStoreFactoryRegistry.class)
public class CacheStoreFactoryRegistryFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      return new CacheStoreFactoryRegistry();
   }
}
