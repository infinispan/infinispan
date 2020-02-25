package org.infinispan.server.core;

import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @author anistor@redhat.com
 * @since 10.1.4
 */
@DefaultFactoryFor(classes = CacheIgnoreManager.class)
@Scope(Scopes.GLOBAL)
public class CacheIgnoreManagerFactory implements ComponentFactory, AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      return new CacheIgnoreManager();
   }
}
