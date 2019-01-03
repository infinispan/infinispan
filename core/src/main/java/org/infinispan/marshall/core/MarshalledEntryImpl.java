package org.infinispan.marshall.core;


import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.metadata.InternalMetadata;

/**
 * @author Mircea Markus
 * @since 6.0
 * @deprecated please utilise {@link MarshalledEntryFactory} instead
 */
@Deprecated
public class MarshalledEntryImpl<K,V> extends org.infinispan.marshall.persistence.impl.MarshalledEntryImpl<K,V> {

   private static MarshalledEntry EMPTY = new MarshalledEntryImpl(null, null, (ByteBuffer) null, null);

   /**
    * Returns the value that should be used as an empty MarshalledEntry. This can be useful when a non null value
    * is required.
    * @param <K> key type
    * @param <V> value type
    * @return cached empty marshalled entry
    */
   public static <K, V> MarshalledEntry<K, V> empty() {
      return EMPTY;
   }

   public MarshalledEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, StreamingMarshaller marshaller) {
      super(key, valueBytes, metadataBytes, marshaller);
   }

   public MarshalledEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, StreamingMarshaller marshaller) {
      super(key, valueBytes, metadataBytes, marshaller);
   }

   public MarshalledEntryImpl(K key, V value, InternalMetadata im, StreamingMarshaller sm) {
      super(key, value, im, sm);
   }
}
