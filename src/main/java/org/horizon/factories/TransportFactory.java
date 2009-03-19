package org.horizon.factories;

import org.horizon.CacheException;
import org.horizon.factories.annotations.DefaultFactoryFor;
import org.horizon.remoting.transport.Transport;
import org.horizon.util.Util;

/**
 * Factory for Transport implementations
 *
 * @author Manik Surtani
 * @since 1.0
 */
@DefaultFactoryFor(classes = Transport.class)
public class TransportFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      String transportClass = globalConfiguration.getTransportClass();
      try {
         if (transportClass == null) return null;
         return (T) Util.getInstance(transportClass);
      } catch (Exception e) {
         throw new CacheException("Unable to create transport of type " + transportClass, e);
      }
   }
}
