package org.infinispan.marshall.core;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.MarshallableEntryFactory;

/**
 * Factory for {@link MarshalledEntry}.
 *
 * @since 6.0
 * @deprecated since 10.0, use {@link MarshallableEntryFactory} instead
 */
@Deprecated
public interface MarshalledEntryFactory<K,V> {

   MarshalledEntry<K,V> newMarshalledEntry(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshalledEntry<K,V> newMarshalledEntry(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshalledEntry<K,V> newMarshalledEntry(Object key, Object value, InternalMetadata im);
}
