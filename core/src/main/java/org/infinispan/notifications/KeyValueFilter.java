package org.infinispan.notifications;

import org.infinispan.metadata.Metadata;

/**
 * A filter for keys with their values.
 *
 * @author William Burns
 * @since 7.0
 */
public interface KeyValueFilter<K, V> {

   /**
    * @param key key to test
    * @param value value to use (could be null for the case of removal)
    * @param metadata metadata
    * @return true if the given key is accepted by this filter.
    */
   boolean accept(K key, V value, Metadata metadata);
}
