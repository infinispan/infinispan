package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.dataconversion.MediaType;
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
    * Will both filter the entry and if passed subsequently convert the value to a new value. A returned value of {@code
    * null} will symbolize the value not passing the filter, so ensure your conversion will not return {@code null} if
    * you want this entry to be returned.
    *
    * @param key         The key for the entry that was changed for the event
    * @param oldValue    The previous value before the event takes place
    * @param oldMetadata The old value before the event takes place
    * @param newValue    The new value for the entry after the event takes place
    * @param newMetadata The new metadata for the entry after the event takes place
    * @param eventType   The type of event that is being raised
    * @return A non {@code null} value converted value when it also passes the filter or {@code null} for when it
    * doesn't pass the filter
    */
   C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType);

   @Override
   default MediaType format() {
      return MediaType.APPLICATION_OBJECT;
   }
}
