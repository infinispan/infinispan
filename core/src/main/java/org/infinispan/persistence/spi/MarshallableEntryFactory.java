package org.infinispan.persistence.spi;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * Factory for {@link MarshallableEntry}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface MarshallableEntryFactory<K,V> {

   /**
    * {@code metadataBytes} defaults to null
    * {@code created} defaults to -1
    * {@code lastUsed} defaults to -1
    *
    * @see #create(ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer, long, long)
    */
   MarshallableEntry<K,V> create(ByteBuffer key, ByteBuffer valueBytes);

   /**
    * Creates a {@link MarshallableEntry} using already marshalled objects as arguments
    *
    * @param key                         {@link ByteBuffer} of serialized key object
    * @param valueBytes                  {@link ByteBuffer} of serialized value object
    * @param metadataBytes               {@link ByteBuffer} of serialized metadata object
    * @param internalMetadataBytes{@link ByteBuffer} of serialized internal metadata object
    * @param created                     timestamp of when the entry was created, -1 means this value is ignored
    * @param lastUsed                    timestamp of last time entry was accessed in memory
    * @return {@link MarshallableEntry} instance that lazily handles unmarshalling of keys, values and metadata via the
    * {@link MarshallableEntry#getKey()}, {@link MarshallableEntry#getValue()} and {@link
    * MarshallableEntry#getMetadata()} methods.
    */
   MarshallableEntry<K,V> create(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes,
         ByteBuffer internalMetadataBytes, long created, long lastUsed);

   /**
    * Creates a {@link MarshallableEntry} using a object key and already marshalled value/metadata as arguments
    *
    * @param key                   entry key
    * @param valueBytes            {@link ByteBuffer} of serialized value object
    * @param metadataBytes         {@link ByteBuffer} of serialized metadata object
    * @param internalMetadataBytes {@link ByteBuffer} of serialized internal metadata object
    * @param created               timestamp of when the entry was created, -1 means this value is ignored
    * @param lastUsed              timestamp of last time entry was accessed in memory
    * @return {@link MarshallableEntry} instance that lazily handles unmarshalling of values and metadata via the {@link
    * MarshallableEntry#getKey()}, {@link MarshallableEntry#getValue()} and {@link MarshallableEntry#getMetadata()}
    * methods.
    */
   MarshallableEntry<K, V> create(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes,
         ByteBuffer internalMetadataBytes, long created, long lastUsed);

   /**
    * {@code value} defaults to null
    *
    * @see #create(Object, Object)
    */
   MarshallableEntry<K,V> create(Object key);

   /**
    * {@code metadata} defaults to null {@code created} defaults to -1 {@code lastUsed} defaults to -1
    *
    * @see #create(Object, Object, Metadata, PrivateMetadata, long, long)
    */
   MarshallableEntry<K,V> create(Object key, Object value);

   /**
    * Creates a {@link MarshallableEntry} using non-marshalled POJOs as arguments
    *
    * @param key              entry key
    * @param value            entry value
    * @param metadata         entry metadata
    * @param internalMetadata entry internal metadata
    * @param created          timestamp of when the entry was created, -1 means this value is ignored
    * @param lastUsed         timestamp of last time entry was accessed in memory
    * @return {@link MarshallableEntry} instance that lazily handles serialization of keys, values and metadata via the
    * {@link MarshallableEntry#getKeyBytes()}, {@link MarshallableEntry#getValueBytes()} and {@link
    * MarshallableEntry#getMetadataBytes()} methods.
    */
   MarshallableEntry<K, V> create(Object key, Object value, Metadata metadata, PrivateMetadata internalMetadata,
         long created, long lastUsed);

   /**
    * Creates a {@link MarshallableEntry} instance from a {@code key} and an {@link InternalCacheValue}.
    *
    * @param key the entry key.
    * @param v   the {@link InternalCacheValue}.
    */
   default MarshallableEntry<K, V> create(Object key, InternalCacheValue<V> v) {
      return create(key, v.getValue(), v.getMetadata(), v.getInternalMetadata(), v.getCreated(), v.getLastUsed());
   }

   /**
    * Creates a {@link MarshallableEntry} instance from an {@link InternalCacheEntry}.
    *
    * @param e the {@link InternalCacheEntry}.
    */
   default MarshallableEntry<K, V> create(InternalCacheEntry<K, V> e) {
      return create(e.getKey(), e.getValue(), e.getMetadata(), e.getInternalMetadata(), e.getCreated(),
            e.getLastUsed());
   }

   /**
    * Creates a {@link MarshallableEntry} using a Key {@link MarshalledValue}.
    *
    * @param key   entry key
    * @param value a {@link MarshalledValue} whose values are used to populate {@link MarshallableEntry#getValueBytes()},
    *              {@link MarshallableEntry#getMetadataBytes()}, {@link MarshallableEntry#created()} and {@link
    *              MarshallableEntry#lastUsed()} fields.
    * @return {@link MarshallableEntry} instance that lazily handles unmarshalling of keys, values and metadata via the
    * {@link MarshallableEntry#getKey()}, {@link MarshallableEntry#getValue()} and {@link
    * MarshallableEntry#getMetadata()} methods.
    * @throws {@link NullPointerException} if the provided {@link MarshalledValue} is null.
    */
   MarshallableEntry<K, V> create(Object key, MarshalledValue value);

   /**
    * Clone the provided MarshallableEntry if needed to apply lifespan expiration. If the entry already has lifespan
    * applied this method will do nothing, returning the same MarshallableEntry back.
    *
    * @param me the entry to clone if applicable
    * @param creationTime the creation time to apply for lifespan
    * @param lifespan the duration for which the entry will expire after the creationTime
    * @return a new entry if lifespan was applied otherwise the same entry provided
    */
   MarshallableEntry<K, V> cloneWithExpiration(MarshallableEntry<K, V> me, long creationTime, long lifespan);

   /**
    * @return a cached empty {@link MarshallableEntry} instance.
    */
   MarshallableEntry<K, V> getEmpty();
}
