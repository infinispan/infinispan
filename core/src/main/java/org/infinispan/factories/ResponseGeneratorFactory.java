package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.responses.DefaultResponseGenerator;
import org.infinispan.remoting.responses.DistributionResponseGenerator;
import org.infinispan.remoting.responses.ResponseGenerator;

/**
 * Creates a ResponseGenerator
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = ResponseGenerator.class)
public class ResponseGeneratorFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.getCacheMode().isDistributed())
         return (T) new DistributionResponseGenerator();
      else
         return (T) new DefaultResponseGenerator();
   }
}
