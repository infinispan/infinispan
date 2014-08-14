package org.infinispan.filter;

import org.infinispan.metadata.Metadata;

/**
 * This interface is an optimization that can be used when a filter and converter are most efficiently used as the same
 * object composing the filtering and conversion in the same method invocation.
 * @author wburns
 * @since 7.0
 */
public interface KeyValueFilterConverter<K, V, C> extends KeyValueFilter<K, V>, Converter<K, V, C> {
   /**
    * Will both filter the entry and if passed subsequently convert the value to a new value.  A returned value of null
    * will symbolize the value not passing the filter, so ensure your conversion will not return null if you want this
    * entry to be returned.
    * @param key The key of the entry to filter
    * @param value The value of the entry to filter and then convert
    * @param metadata The metadata attached to the entry
    * @return The converted value or null if the filter didn't pass
    */
   public C filterAndConvert(K key, V value, Metadata metadata);
}
