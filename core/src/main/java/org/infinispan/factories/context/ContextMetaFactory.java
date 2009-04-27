package org.infinispan.factories.context;

import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * Builds a context factory
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = ContextFactory.class)
public class ContextMetaFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.getCacheMode().isDistributed())
         return (T) new DistContextFactory();
      else
         return (T) new DefaultContextFactory();
   }
}
