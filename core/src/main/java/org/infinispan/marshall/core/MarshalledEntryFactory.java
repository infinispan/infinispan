package org.infinispan.marshall.core;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.metadata.InternalMetadata;

/**
 * Factory for {@link MarshalledEntry}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface MarshalledEntryFactory<K,V> {

   MarshalledEntry<K,V> newMarshalledEntry(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshalledEntry<K,V> newMarshalledEntry(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshalledEntry<K,V> newMarshalledEntry(Object key, Object value, InternalMetadata im);
}
