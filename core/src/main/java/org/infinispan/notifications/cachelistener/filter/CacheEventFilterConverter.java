package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.metadata.Metadata;

/**
 * This interface is an optimization that can be used when an event filter and converter are most efficiently used as
 * the same object composing the filtering and conversion in the same method invocation.
 *
 * @author wburns
 * @since 7.0
 */
public interface CacheEventFilterConverter<K, V, C> extends CacheEventFilter<K, V>, CacheEventConverter<K, V, C> {
   /**
    * Will both filter the entry and if passed subsequently convert the value to a new value.  A returned value of null
    * will symbolize the value not passing the filter, so ensure your conversion will not return null if you want this
    * entry to be returned.
    * @param key The key for the entry that was changed for the event
    * @param oldValue The previous value before the event takes place
    * @param oldMetadata The old value before the event takes place
    * @param newValue The new value for the entry after the event takes place
    * @param newMetadata The new metadata for the entry after the event takes place
    * @param eventType The type of event that is being raised
    * @return A non null value converted value when it also passes the filter or null for when it doesn't pass the filter
    */
   public C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata,
                             EventType eventType);
}
