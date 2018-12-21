package org.infinispan.persistence.spi;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.metadata.Metadata;

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
    * @see #create(ByteBuffer, ByteBuffer, ByteBuffer, long, long)
    */
   MarshallableEntry<K,V> create(ByteBuffer key, ByteBuffer valueBytes);

   /**
    * Creates a {@link MarshallableEntry} using already marshalled objects as arguments
    *
    * @param key           {@link ByteBuffer} of serialized key object
    * @param valueBytes    {@link ByteBuffer} of serialized value object
    * @param metadataBytes {@link ByteBuffer} of serialized metadata object
    * @param created       timestamp of when the entry was created, -1 means this value is ignored
    * @param lastUsed      timestamp of last time entry was accessed in memory
    * @return {@link MarshallableEntry} instance that lazily handles unmarshalling of keys, values and metadata via the
    * {@link MarshallableEntry#getKey()}, {@link MarshallableEntry#getValue()} and {@link MarshallableEntry#getMetadata()}
    * methods.
    */
   MarshallableEntry<K,V> create(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed);

   /**
    * Creates a {@link MarshallableEntry} using a object key and already marshalled value/metadata as arguments
    *
    * @param key           entry key
    * @param valueBytes    {@link ByteBuffer} of serialized value object
    * @param metadataBytes {@link ByteBuffer} of serialized metadata object
    * @param created       timestamp of when the entry was created, -1 means this value is ignored
    * @param lastUsed      timestamp of last time entry was accessed in memory
    * @return {@link MarshallableEntry} instance that lazily handles unmarshalling of values and metadata via the {@link
    * MarshallableEntry#getKey()}, {@link MarshallableEntry#getValue()} and {@link MarshallableEntry#getMetadata()}
    * methods.
    */
   MarshallableEntry<K,V> create(Object key, ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed);

   /**
    * {@code value} defaults to null
    *
    * @see #create(Object, Object)
    */
   MarshallableEntry<K,V> create(Object key);

   /**
    * {@code metadata} defaults to null
    * {@code created} defaults to -1
    * {@code lastUsed} defaults to -1
    *
    * @see #create(Object, Object, Metadata, long, long)
    */
   MarshallableEntry<K,V> create(Object key, Object value);

   /**
    * Creates a {@link MarshallableEntry} using non-marshalled POJOs as arguments
    *
    * @param key      entry key
    * @param value    entry value
    * @param metadata entry metadata
    * @param created  timestamp of when the entry was created, -1 means this value is ignored
    * @param lastUsed timestamp of last time entry was accessed in memory
    * @return {@link MarshallableEntry} instance that lazily handles serialization of keys, values and metadata via the
    * {@link MarshallableEntry#getKeyBytes()}, {@link MarshallableEntry#getValueBytes()} and {@link
    * MarshallableEntry#getMetadataBytes()} methods.
    */
   MarshallableEntry<K,V> create(Object key, Object value, Metadata metadata, long created, long lastUsed);

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
   MarshallableEntry<K,V> create(Object key, MarshalledValue value);

   /**
    * @return a cached empty {@link MarshallableEntry} instance.
    */
   MarshallableEntry<K,V> getEmpty();
}
