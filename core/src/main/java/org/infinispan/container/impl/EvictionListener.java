package org.infinispan.container.impl;

import org.infinispan.container.entries.InternalCacheEntry;

public interface EvictionListener<K, V> {
   /**
    * Method invoked every time an entry is evicted from the cache along with its key. Note that when the <b>pre</b>
    * value is <b>true</b> the entry is still in the cache and the lock is held for this entry. When invoked with
    * <b>pre</b> being <b>false</b> is after the fact without the lock held. If any exception is thrown during this
    * invocation it is not deterministic if it will stop the operation and may only be logged.
    * @param pre Whether this is just before the eviction completes or not.
    * @param key The key tied the removed entry
    * @param value The entry that is removed due to eviction
    */
   void onEntryChosenForEviction(boolean pre, K key, InternalCacheEntry<K, V> value);
}
