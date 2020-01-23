package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.util.PeekableMap;

public interface PeekableTouchableMap<K, V> extends PeekableMap<K, V>, TouchableMap, ConcurrentMap<K, V> {
}
