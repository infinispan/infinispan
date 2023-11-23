package org.infinispan.cache.impl;

import org.infinispan.factories.ComponentRegistry;

/**
 * This interface is used to hold methods that are only available to internal implementations.
 * @since 15.0
 **/
public interface InternalCache<K, V> {
   ComponentRegistry getComponentRegistry();
}
