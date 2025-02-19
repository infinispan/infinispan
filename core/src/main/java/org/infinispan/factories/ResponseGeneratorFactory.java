package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.responses.ResponseGenerator;

/**
 * Creates a ResponseGenerator
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = ResponseGenerator.class)
public class ResponseGeneratorFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      return ResponseGenerator.INSTANCE;
   }
}
