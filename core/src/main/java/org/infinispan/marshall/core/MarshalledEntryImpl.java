package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class MarshalledEntryImpl<K,V> implements MarshalledEntry<K,V> {

   private ByteBuffer keyBytes;
   private ByteBuffer valueBytes;
   private ByteBuffer metadataBytes;
   private transient K key;
   private transient V value;
   private transient InternalMetadata metadata;
   private final transient StreamingMarshaller marshaller;

   public MarshalledEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, StreamingMarshaller marshaller) {
      this.keyBytes = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   public MarshalledEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, StreamingMarshaller marshaller) {
      this.key = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   public MarshalledEntryImpl(K key, V value, InternalMetadata im, StreamingMarshaller sm) {
      this.key = key;
      this.value = value;
      this.metadata = im;
      this.marshaller = sm;
   }

   @Override
   public K getKey() {
      if (key == null) {
         if (keyBytes == null) {
            return null;
         }
         key = unmarshall(keyBytes);
      }
      return key;
   }

   @Override
   public V getValue() {
      if (value == null) {
         if (valueBytes == null) {
            return null;
         }
         value = unmarshall(valueBytes);
      }
      return value;
   }

   @Override
   public InternalMetadata getMetadata() {
      if (metadata == null) {
         if (metadataBytes == null)
            return null;
         else
            metadata = unmarshall(metadataBytes);
      }
      return metadata;
   }

   @Override
   public ByteBuffer getKeyBytes() {
      if (keyBytes == null) {
         if (key == null) {
            return null;
         }
         keyBytes = marshall(key);
      }
      return keyBytes;
   }

   @Override
   public ByteBuffer getValueBytes() {
      if (valueBytes == null) {
         if (value == null) {
            return null;
         }
         valueBytes = marshall(value);
      }
      return valueBytes;
   }

   @Override
   public ByteBuffer getMetadataBytes() {
      if (metadataBytes == null) {
         if  (metadata == null)
            return null;
         metadataBytes = marshall(metadata);
      }
      return metadataBytes;
   }

   private ByteBuffer marshall(Object obj) {
      try {
         return marshaller.objectToBuffer(obj);
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @SuppressWarnings(value = "unchecked")
   private <T> T unmarshall(ByteBuffer buf) {
      try {
         return (T) marshaller.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MarshalledEntryImpl)) return false;

      MarshalledEntryImpl that = (MarshalledEntryImpl) o;

      if (getKeyBytes() != null ? !getKeyBytes().equals(that.getKeyBytes()) : that.getKeyBytes() != null) return false;
      if (getMetadataBytes() != null ? !getMetadataBytes().equals(that.getMetadataBytes()) : that.getMetadataBytes() != null) return false;
      if (getValueBytes() != null ? !getValueBytes().equals(that.getValueBytes()) : that.getValueBytes() != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = getKeyBytes() != null ? getKeyBytes().hashCode() : 0;
      result = 31 * result + (getValueBytes() != null ? getValueBytes().hashCode() : 0);
      result = 31 * result + (getMetadataBytes() != null ? getMetadataBytes().hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "MarshalledEntryImpl{" +
            "keyBytes=" + keyBytes +
            ", valueBytes=" + valueBytes +
            ", metadataBytes=" + metadataBytes +
            ", key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            ", marshaller=" + marshaller +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<MarshalledEntryImpl> {

      private static final long serialVersionUID = -5291318076267612501L;

      private final StreamingMarshaller marshaller;

      public Externalizer(StreamingMarshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public void writeObject(ObjectOutput output, MarshalledEntryImpl me) throws IOException {
         output.writeObject(me.getKeyBytes());
         output.writeObject(me.getValueBytes());
         output.writeObject(me.getMetadataBytes());
      }

      @Override
      public MarshalledEntryImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            ByteBuffer keyBytes = (ByteBuffer) input.readObject();
            ByteBuffer valueBytes = (ByteBuffer) input.readObject();
            ByteBuffer metadataBytes = (ByteBuffer) input.readObject();
            return new MarshalledEntryImpl(keyBytes, valueBytes, metadataBytes, marshaller);
      }

      @Override
      public Integer getId() {
         return Ids.MARSHALLED_ENTRY_ID;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends MarshalledEntryImpl>> getTypeClasses() {
         return Util.<Class<? extends MarshalledEntryImpl>>asSet(MarshalledEntryImpl.class);
      }
   }
}
