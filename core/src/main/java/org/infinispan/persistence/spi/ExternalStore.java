package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;

/**
 * Basic interface for interacting with an external store in a read-write mode.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface ExternalStore<K, V> extends CacheLoader<K, V>, CacheWriter<K, V> {
}
