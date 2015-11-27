package org.infinispan.factories;

import org.infinispan.commons.hash.Hash;
import org.infinispan.factories.annotations.DefaultFactoryFor;

/**
 * @author Radim Vansa &ltrvansa@redhat.com&gt;
 */
@DefaultFactoryFor(classes = Hash.class)
public class HashFunctionFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      return (T) configuration.clustering().hash().hash();
   }
}
