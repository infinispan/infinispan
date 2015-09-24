package org.infinispan.security.impl;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.security.GlobalSecurityManager;

/**
 * Factory for GlobalSecurityManager implementations
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = GlobalSecurityManager.class)
public class GlobalSecurityManagerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (globalConfiguration.security().authorization().enabled())
         return componentType.cast(new GlobalSecurityManagerImpl());
      else
         return null;
   }
}
