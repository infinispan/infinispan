package org.infinispan.marshall.core;

import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * Defines an externally persisted entry. External stores that keep the data in serialised form should return an
 * MarshalledEntry that contains the data in binary form (ByteBuffer) and unmarshall it lazily when
 * getKey/Value/Metadata are invoked. This approach avoids unnecessary object (de)serialization e.g
 * when the entries are fetched from the external store for the sole purpose of being sent over the wire to
 * another requestor node.
 *
 * @author Mircea Markus
 * @since 6.0
 * @deprecated since 10.0, use {@link MarshallableEntry}
 */
@Deprecated
public interface MarshalledEntry<K,V> extends MarshallableEntry<K,V> {

   @Override
   InternalMetadata getMetadata();
}
