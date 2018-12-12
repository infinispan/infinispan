package org.infinispan.persistence.spi;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.metadata.InternalMetadata;

/**
 * Factory for {@link MarshallableEntry}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface MarshallableEntryFactory<K,V> {

   MarshallableEntry<K,V> create(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshallableEntry<K,V> create(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes);

   MarshallableEntry<K,V> create(Object key, Object value, InternalMetadata im);

   /**
    * @return a cached empty {@link MarshallableEntry} instance.
    */
   MarshallableEntry<K,V> getEmpty();
}
