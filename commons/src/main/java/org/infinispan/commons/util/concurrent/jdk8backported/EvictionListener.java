package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.Map;

public interface EvictionListener<K, V> {

    void onEntryEviction(Map<K, V> evicted);

    void onEntryChosenForEviction(Map.Entry<K, V> entry);

    void onEntryActivated(Object key);

    void onEntryRemoved(Map.Entry<K, V> entry);
}
