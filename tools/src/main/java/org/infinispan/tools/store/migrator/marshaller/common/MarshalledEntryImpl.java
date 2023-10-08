package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class MarshalledEntryImpl<K,V> {

   private ByteBuffer keyBytes;
   private ByteBuffer valueBytes;
   private ByteBuffer metadataBytes;
   private transient K key;
   private transient V value;
   private transient InternalMetadata metadata;
   private final transient Marshaller marshaller;

   MarshalledEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, Marshaller marshaller) {
      this.keyBytes = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   public K getKey() {
      if (key == null) {
         if (keyBytes == null) {
            return null;
         }
         key = unmarshall(keyBytes);
      }
      return key;
   }

   public V getValue() {
      if (value == null) {
         if (valueBytes == null) {
            return null;
         }
         value = unmarshall(valueBytes);
      }
      return value;
   }

   public InternalMetadata getMetadata() {
      if (metadata == null) {
         if (metadataBytes == null)
            return null;
         else
            metadata = unmarshall(metadataBytes);
      }
      return metadata;
   }

   public ByteBuffer getKeyBytes() {
      if (keyBytes == null) {
         if (key == null) {
            return null;
         }
         keyBytes = marshall(key);
      }
      return keyBytes;
   }

   public ByteBuffer getValueBytes() {
      if (valueBytes == null) {
         if (value == null) {
            return null;
         }
         valueBytes = marshall(value);
      }
      return valueBytes;
   }

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

   public static class Externalizer extends AbstractMigratorExternalizer<MarshalledEntryImpl> {

      private final Marshaller marshaller;

      public Externalizer(Marshaller marshaller) {
         super(MarshalledEntryImpl.class, Ids.MARSHALLED_ENTRY_ID);
         this.marshaller = marshaller;
      }

      @Override
      public MarshalledEntryImpl readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         ByteBuffer keyBytes = (ByteBuffer) input.readObject();
         ByteBuffer valueBytes = (ByteBuffer) input.readObject();
         ByteBuffer metadataBytes = (ByteBuffer) input.readObject();
         return new MarshalledEntryImpl(keyBytes, valueBytes, metadataBytes, marshaller);
      }
   }
}
