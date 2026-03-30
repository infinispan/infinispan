package org.infinispan.topology;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = {
      ClusterTopologyManager.class,
      LocalTopologyManager.class,
      OrderedGracefulLeaveHandler.class,
})
public class TopologyComponentFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (globalConfiguration.transport().transport() == null)
         return null;

      if (componentName.equals(ClusterTopologyManager.class.getName()))
         return new ClusterTopologyManagerImpl();

      if (componentName.equals(LocalTopologyManager.class.getName()))
         return new LocalTopologyManagerImpl();

      if (componentName.equals(OrderedGracefulLeaveHandler.class.getName()))
         return new OrderedGracefulLeaveHandler();

      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
