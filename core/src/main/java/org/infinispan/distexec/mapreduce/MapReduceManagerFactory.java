package org.infinispan.distexec.mapreduce;

import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * MapReduceManagerFactory is a default factory class for {@link MapReduceManager}.
 * <p>
 * This is an internal class, not intended to be used by clients.
 * @author Vladimir Blagojevic
 * @since 5.2
 */
@DefaultFactoryFor(classes={MapReduceManager.class})
public class MapReduceManagerFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   @Override
   public <T> T construct(Class<T> componentType) {      
      T result = (T) new MapReduceManagerImpl();
      return result;
   }
}
