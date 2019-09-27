package org.infinispan.topology;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Factory for ClusterTopologyManager implementations.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = ClusterTopologyManager.class)
public class ClusterTopologyManagerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (globalConfiguration.transport().transport() == null)
         return null;

      return new ClusterTopologyManagerImpl();
   }
}
