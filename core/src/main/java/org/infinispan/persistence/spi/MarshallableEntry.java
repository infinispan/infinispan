package org.infinispan.persistence.spi;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;

/**
 * Defines an externally persisted entry. External stores that keep the data in serialised form should return an
 * MarshalledEntry that contains the data in binary form (ByteBuffer) and unmarshall it lazily when
 * getKey/Value/Metadata are invoked. This approach avoids unnecessary object (de)serialization e.g
 * when the entries are fetched from the external store for the sole purpose of being sent over the wire to
 * another requestor node.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface MarshallableEntry<K, V> {

   /**
    * Returns the key in serialized format.
    */
   ByteBuffer getKeyBytes();

   /**
    * Returns the value in serialize format.
    */
   ByteBuffer getValueBytes();

   /**
    * @return null if there's no metadata associated with the object (e.g. expiry info, version..)
    */
   ByteBuffer getMetadataBytes();

   /**
    * Returns the same key as {@link #getKeyBytes()}, but unmarshalled.
    */
   K getKey();

   /**
    * Returns the same value as {@link #getKeyBytes()}, but unmarshalled.
    */
   V getValue();

   /**
    * @return might be null if there's no metadata associated with the object (e.g. expiry info, version..).
    */
   Metadata getMetadata();

   long created();

   long lastUsed();

   boolean isExpired(long now);

   long expiryTime();

   /**
    * A bridge method required to ensure backwards compatibility with old store implementations that rely on {@link
    * MarshalledEntry}. It's not possible to simply create a static wrapper class around a {@link MarshallableEntry} as
    * {@link MarshalledEntry#getMetadataBytes()} is different from {@link MarshalledEntry#getMetadataBytes()} and we
    * require a {@link org.infinispan.commons.marshall.Marshaller} instance to generate said bytes.
    *
    * @deprecated This should not be used by users and will be removed in subsequent versions along with {@link
    * MarshalledEntry}.
    */
   @Deprecated
   default MarshalledEntry<K, V> asMarshalledEntry() {
      return null;
   }

   MarshalledValue getMarshalledValue();
}
