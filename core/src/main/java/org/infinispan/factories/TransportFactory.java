package org.infinispan.factories;

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.Util;

/**
 * Factory for Transport implementations
 *
 * @author Manik Surtani
 * @since 4.0
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
