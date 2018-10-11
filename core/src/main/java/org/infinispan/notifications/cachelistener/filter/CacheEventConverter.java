package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Experimental;
import org.infinispan.metadata.Metadata;

/**
 * A converter that can be used to convert the value given for an event.  This converter allows for converting based
 * on the previous value as well as the new updated value.  The old value and old metadata are the previous values and
 * the new value and new metadata are the new values even for pre and post events.
 * @author wburns
 * @since 7.0
 */
public interface CacheEventConverter<K, V, C> {
   /**
    * Converts the given newValue into something different possibly.
    * @param key The key for the entry that was changed for the event
    * @param oldValue The previous value before the event takes place
    * @param oldMetadata The old value before the event takes place
    * @param newValue The new value for the entry after the event takes place
    * @param newMetadata The new metadata for the entry after the event takes place
    * @param eventType The type of event that is being raised
    * @return The converted value to be used in the event
    */
   public C convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType);

   default MediaType format() {
      return MediaType.APPLICATION_OBJECT;
   }

   /**
    * @return if true, {@link #convert(Object, Object, Metadata, Object, Metadata, EventType)} will be presented with data
    * in the request format rather than the format specified in {@link #format()}. The request format is defined as the MediaType
    * that a cache was previously decorated with {@link org.infinispan.AdvancedCache#withMediaType(String, String)}.
    */
   @Experimental
   default boolean useRequestFormat() {
      return false;
   }

}
