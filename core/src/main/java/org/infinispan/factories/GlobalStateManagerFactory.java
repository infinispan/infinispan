package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.impl.GlobalStateManagerImpl;

/**
 * GlobalStateManagerFactory.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@DefaultFactoryFor(classes = GlobalStateManager.class)
public class GlobalStateManagerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (globalConfiguration.globalState().enabled())
         return componentType.cast(new GlobalStateManagerImpl());
      else
         return null;
   }

}
