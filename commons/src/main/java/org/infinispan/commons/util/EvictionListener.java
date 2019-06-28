package org.infinispan.commons.util;

import java.util.Map;

/**
 * Eviction listener that is notified when entries are evicted from the underlying container due
 * to the given eviction policy.
 * @author wburns
 * @since 9.0
 * @deprecated since 10.0 - This class is not used internally anymore
 */
@Deprecated
public interface EvictionListener<K, V> {

   /**
    * Called back after entries have been evicted
    * @param evicted
    */
   void onEntryEviction(Map<K, V> evicted);

   /**
    * Called back before an entry is evicted
    * @param entry
    */
   void onEntryChosenForEviction(Map.Entry<K, V> entry);

   /**
    * Called back when an entry has been activated
    * @param key
    */
   void onEntryActivated(Object key);

   /**
    * Called when an entry is specifically removed from the container.
    * @param entry
    */
   void onEntryRemoved(Map.Entry<K, V> entry);
}
