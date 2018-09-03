package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.transport.Transport;

/**
 * Factory for Transport implementations
 *
 * @author Manik Surtani
 * @since 4.0
 */
@DefaultFactoryFor(classes = Transport.class)
public class TransportFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      return globalConfiguration.transport().transport();
   }

}
