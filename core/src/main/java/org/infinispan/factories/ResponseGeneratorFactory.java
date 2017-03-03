package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.responses.DefaultResponseGenerator;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.responses.TriangleResponseGenerator;

/**
 * Creates a ResponseGenerator
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = ResponseGenerator.class)
public class ResponseGeneratorFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().cacheMode().isDistributed() &&
            !configuration.transaction().transactionMode().isTransactional()) {
         return componentType.cast(new TriangleResponseGenerator());
      } else {
         return componentType.cast(new DefaultResponseGenerator());
      }
   }
}
