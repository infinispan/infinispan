package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.metadata.Metadata;

/**
 * A filter that can be used to tell if an event should be raised or not.  This filter allows for filtering based
 * on the previous value as well as the new updated value.  The old value and old metadata are the previous values and
 * the new value and new metadata are the new values even for pre and post events.
 *
 * @author wburns
 * @since 7.0
 */
public interface CacheEventFilter<K, V> {
   /**
    * Whether this event should be raised to the listener it is attached to.
    * @param key The key for the entry that was changed for the event
    * @param oldValue The previous value before the event takes place
    * @param oldMetadata The old value before the event takes place
    * @param newValue The new value for the entry after the event takes place
    * @param newMetadata The new metadata for the entry after the event takes place
    * @param eventType The type of event that is being raised
    * @return Whether to notify the listener
    */
   boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType);

   /**
    * @return The desired data format to be used in the accept operation. If null, the filter will receive data as it's stored.
    */
   default MediaType format() {
      return MediaType.APPLICATION_OBJECT;
   }
}
