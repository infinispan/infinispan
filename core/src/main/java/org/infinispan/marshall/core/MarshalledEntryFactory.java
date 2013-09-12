package org.infinispan.marshall.core;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.metadata.InternalMetadata;

/**
 * Factory for {@link MarshalledEntry}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface MarshalledEntryFactory {

   MarshalledEntry newMarshalledEntry(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshalledEntry newMarshalledEntry(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshalledEntry newMarshalledEntry(Object key, Object value, InternalMetadata im);
}
