package org.infinispan.persistence.remote.internal;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.persistence.remote.global.GlobalRemoteContainers;

/**
 * @since 15.0
 */
@DefaultFactoryFor(classes = GlobalRemoteContainers.class)
public class RemoteContainersComponentFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(GlobalRemoteContainers.class.getName()))
         return new GlobalRemoteContainersImpl();
      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
