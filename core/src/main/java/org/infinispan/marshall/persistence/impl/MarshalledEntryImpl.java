package org.infinispan.marshall.persistence.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.protostream.MessageMarshaller;

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
   private final transient org.infinispan.commons.marshall.Marshaller marshaller;

   MarshalledEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, org.infinispan.commons.marshall.Marshaller marshaller) {
      this.keyBytes = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   MarshalledEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, org.infinispan.commons.marshall.Marshaller marshaller) {
      this.key = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   MarshalledEntryImpl(K key, V value, InternalMetadata im, org.infinispan.commons.marshall.Marshaller sm) {
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
      StringBuilder sb = new StringBuilder().append("MarshalledEntryImpl{")
            .append("keyBytes=").append(keyBytes)
            .append(", valueBytes=").append(valueBytes)
            .append(", metadataBytes=").append(metadataBytes)
            .append(", key=").append(key);
      if (key == null && keyBytes != null && marshaller != null) {
         sb.append('/').append(this.<Object>unmarshall(keyBytes));
      }
      sb.append(", value=").append(value);
      if (value == null && valueBytes != null && marshaller != null) {
         sb.append('/').append(this.<Object>unmarshall(valueBytes));
      }
      sb.append(", metadata=").append(metadata);
      if (metadata == null && metadataBytes != null && marshaller != null) {
         sb.append('/').append(this.<Object>unmarshall(metadataBytes));
      }
      sb.append(", marshaller=").append(marshaller).append('}');
      return sb.toString();
   }

   public static class Externalizer extends AbstractExternalizer<MarshalledEntryImpl> {

      private static final long serialVersionUID = -5291318076267612501L;

      private final org.infinispan.commons.marshall.Marshaller marshaller;

      public Externalizer(org.infinispan.commons.marshall.Marshaller marshaller) {
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

   public static class Marshaller implements MessageMarshaller<MarshalledEntryImpl> {

      private final org.infinispan.commons.marshall.Marshaller marshaller;

      public Marshaller(org.infinispan.commons.marshall.Marshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public MarshalledEntryImpl readFrom(ProtoStreamReader reader) throws IOException {
         ByteBuffer keyBytes = reader.readObject("key", ByteBufferImpl.class);
         ByteBuffer valueBytes = reader.readObject("value", ByteBufferImpl.class);
         ByteBuffer metadataBytes = reader.readObject("metadata", ByteBufferImpl.class);
         return new MarshalledEntryImpl(keyBytes, valueBytes, metadataBytes, marshaller);
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, MarshalledEntryImpl marshalledEntry) throws IOException {
         writer.writeObject("key", marshalledEntry.getKeyBytes(), ByteBufferImpl.class);
         writer.writeObject("value", marshalledEntry.getValueBytes(), ByteBufferImpl.class);
         writer.writeObject("metadata", marshalledEntry.getMetadataBytes(), ByteBufferImpl.class);
      }

      @Override
      public Class<? extends MarshalledEntryImpl> getJavaClass() {
         return MarshalledEntryImpl.class;
      }

      @Override
      public String getTypeName() {
         return "persistence.MarshalledEntry";
      }
   }
}
