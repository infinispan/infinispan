package org.infinispan.server.hotrod.event;

import org.infinispan.filter.KeyValueFilter;

/**
 * @author Galder Zamarreño
 */
public interface KeyValueFilterFactory {

   /**
    * Retrieves a key/value filter instance from this factory.
    *
    * @param params parameters for the factory to be used to create filter instances
    * @return a filter instance for keys with their values
    */
   <K, V> KeyValueFilter<K, V> getKeyValueFilter(Object[] params);

}
