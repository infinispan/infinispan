package org.infinispan.marshall.core;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class MarshalledEntryImpl<K,V> implements MarshalledEntry<K,V> {

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

   private ByteBuffer keyBytes;
   private ByteBuffer valueBytes;
   private ByteBuffer metadataBytes;
   private transient K key;
   private transient V value;
   private transient Metadata metadata;
   private volatile transient Marshaller marshaller;

   public MarshalledEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, Marshaller marshaller) {
      this.keyBytes = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   public MarshalledEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, Marshaller marshaller) {
      this.key = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.marshaller = marshaller;
   }

   public MarshalledEntryImpl(K key, V value, Metadata im, Marshaller sm) {
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
      Metadata metadata = metadata();
      return metadata instanceof InternalMetadata ? (InternalMetadata) metadata : null;
   }

   // Internal method required so that command externalizers can make use of MarshalledEntryImpl to serialize
   // user key/value/metadata via the User marshaller
   public Metadata metadata() {
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
         throw new MarshallingException(e);
      }
   }

   @SuppressWarnings(value = "unchecked")
   private <T> T unmarshall(ByteBuffer buf) {
      try {
         return (T) marshaller.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
      } catch (Exception e) {
         throw new MarshallingException(e);
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

      private final Marshaller userMarshaller;

      public Externalizer(Marshaller userMarshaller) {
         this.userMarshaller = userMarshaller;
      }

      @Override
      public void writeObject(UserObjectOutput output, MarshalledEntryImpl me) throws IOException {
         output.writeObject(me.getKeyBytes());
         output.writeObject(me.getValueBytes());
         output.writeObject(me.getMetadataBytes());
      }

      @Override
      public MarshalledEntryImpl readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         ByteBuffer keyBytes = (ByteBuffer) input.readObject();
         ByteBuffer valueBytes = (ByteBuffer) input.readObject();
         ByteBuffer metadataBytes = (ByteBuffer) input.readObject();
         return new MarshalledEntryImpl(keyBytes, valueBytes, metadataBytes, userMarshaller);
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
