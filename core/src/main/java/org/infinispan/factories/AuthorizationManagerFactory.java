package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.impl.AuthorizationManagerImpl;

@DefaultFactoryFor(classes = AuthorizationManager.class)
public class AuthorizationManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.security().enabled())
         return (T) new AuthorizationManagerImpl();
      else
         return null;
   }
}
