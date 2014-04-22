package org.infinispan.filter;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.metadata.Metadata;

/**
 * A filter for keys with their values.  This class is complemented by the {@link org.infinispan.filter.KeyFilter}
 * class.  This filter should be used in most cases over {@link org.infinispan.filter.KeyFilter} since it allows
 * for more control of the filtering since it provides the ability to filter on the value and metadata in addition
 * to the key.
 *
 * @author William Burns
 * @since 7.0
 */
@ThreadSafe
public interface KeyValueFilter<K, V> {
   /**
    * @param key key to test
    * @param value value to use (could be null for the case of removal)
    * @param metadata metadata
    * @return true if the given key is accepted by this filter.
    */
   boolean accept(K key, V value, Metadata metadata);
}
