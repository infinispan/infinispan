package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;

/**
 * Advanced interface for interacting with an external store in a read-write mode.
 *
 * @author Mircea Markus
 * @since 6.0
 * @deprecated since 11.0 replaced by {@link NonBlockingStore}
 */
@ThreadSafe
@Deprecated
public interface AdvancedLoadWriteStore<K,V> extends ExternalStore<K,V>, AdvancedCacheLoader<K,V>, AdvancedCacheWriter<K,V> {
}
