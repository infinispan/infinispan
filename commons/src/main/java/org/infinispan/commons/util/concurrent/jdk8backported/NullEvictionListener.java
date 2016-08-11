package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.Map;

class NullEvictionListener<K, V> implements EvictionListener<K, V> {
    @Override
    public void onEntryEviction(Map<K, V> evicted) {
        // Do nothing.
    }

   @Override
    public void onEntryChosenForEviction(Map.Entry<K, V> entry) {
        // Do nothing.
    }

   @Override
    public void onEntryActivated(Object key) {
        // Do nothing.
    }

   @Override
    public void onEntryRemoved(Map.Entry<K, V> entry) {
        // Do nothing.
    }
}
