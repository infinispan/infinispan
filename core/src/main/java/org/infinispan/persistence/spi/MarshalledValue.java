package org.infinispan.persistence.spi;

import org.infinispan.commons.io.ByteBuffer;

/**
 * A marshallable object containing serialized representations of cache values and metadata, that can be used to store
 * values, metadata and timestamps as a single entity.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface MarshalledValue {
   ByteBuffer getValueBytes();

   ByteBuffer getMetadataBytes();

   long getCreated();

   long getLastUsed();
}
