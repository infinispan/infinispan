package org.infinispan.marshall.core;

/**
 * Factory for {@link MarshalledEntry}.
 *
 * @since 6.0
 * @deprecated use {@link org.infinispan.persistence.spi.MarshalledEntryFactory} instead
 */
@Deprecated
public interface MarshalledEntryFactory<K,V> extends org.infinispan.persistence.spi.MarshalledEntryFactory<K,V> {
}
