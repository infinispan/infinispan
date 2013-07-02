package org.infinispan.topology;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Factory for ClusterTopologyManager implementations
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = LocalTopologyManager.class)
public class LocalTopologyManagerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (globalConfiguration.transport().transport() == null)
         return null;

      return (T) new LocalTopologyManagerImpl();
   }

}
