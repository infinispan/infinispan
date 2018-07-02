package org.infinispan.commons.util;

import java.util.concurrent.ConcurrentMap;

public abstract class AbstractDelegatingConcurrentMap<K, V> extends AbstractDelegatingMap<K, V> implements ConcurrentMap<K, V> {

   protected abstract ConcurrentMap<K, V> delegate();
}
