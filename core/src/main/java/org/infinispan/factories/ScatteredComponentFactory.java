package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.scattered.impl.ScatteredVersionManagerImpl;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@DefaultFactoryFor(classes = ScatteredVersionManager.class)
public class ScatteredComponentFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (componentType.equals(ScatteredVersionManager.class)) {
         return (T) new ScatteredVersionManagerImpl();
      }
      throw new CacheConfigurationException("Don't know how to create a " + componentType.getName());
   }
}
