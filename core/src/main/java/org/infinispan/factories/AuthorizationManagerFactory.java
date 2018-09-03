package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.impl.AuthorizationManagerImpl;

@DefaultFactoryFor(classes = AuthorizationManager.class)
public class AuthorizationManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      if (configuration.security().authorization().enabled())
         return new AuthorizationManagerImpl();
      else
         return null;
   }
}
