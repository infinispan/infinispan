package org.infinispan.marshall.core;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.metadata.InternalMetadata;

/**
 * Defines an externally persisted entry. External stores that keep the data in serialised form should return an
 * MarshalledEntry that contains the data in binary form (ByteBuffer) and unmarshall it lazily when
 * getKey/Value/Metadata are invoked. This approach avoids unnecessary object (de)serialization e.g
 * when the entries are fetched from the external store for the sole purpose of being sent over the wire to
 * another requestor node.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface MarshalledEntry<K, V> {

   ByteBuffer getKeyBytes();

   ByteBuffer getValueBytes();

   /**
    * @return null if there's no metadata associated with the object (e.g. expiry info, version..)
    */
   ByteBuffer getMetadataBytes();

   K getKey();

   V getValue();

   /**
    * @return might be null if there's no metadata associated with the object (e.g. expiry info, version..).
    */
   InternalMetadata getMetadata();
}
