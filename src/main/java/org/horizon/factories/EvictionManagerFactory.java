package org.horizon.factories;

import org.horizon.eviction.EvictionManager;
import org.horizon.eviction.EvictionManagerImpl;
import org.horizon.factories.annotations.DefaultFactoryFor;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@DefaultFactoryFor(classes = {EvictionManager.class})
public class EvictionManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.getEvictionConfig() != null) {
         return (T) new EvictionManagerImpl();
      } else return null;
   }
}
