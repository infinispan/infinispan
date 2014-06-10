package org.infinispan.server.hotrod.event;

import org.infinispan.filter.Converter;

/**
 * @author Galder Zamarreño
 */
public interface ConverterFactory {

   /**
    * Retrieves a converter instance from this factory.
    *
    * @param params parameters for the factory to be used to create converter instances
    * @return a {@link org.infinispan.filter.Converter} instance used
    * to reduce size of event payloads
    */
   <K, V, C> Converter<K, V, C> getConverter(Object[] params);

}
