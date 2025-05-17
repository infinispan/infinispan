package org.infinispan.marshall.persistence.impl;

import static java.lang.Math.min;

import java.util.Objects;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.PersistenceException;

/**
 * @author Ryan Emerson
 * @since 10.0
 */
public class MarshallableEntryImpl<K, V> implements MarshallableEntry<K, V> {

   long created;
   long lastUsed;
   ByteBuffer valueBytes;
   ByteBuffer metadataBytes;
   ByteBuffer internalMetadataBytes;
   volatile ByteBuffer keyBytes;
   transient volatile K key;
   transient volatile V value;
   transient volatile Metadata metadata;
   transient volatile PrivateMetadata internalMetadata;
   transient org.infinispan.commons.marshall.Marshaller marshaller;

   MarshallableEntryImpl() {}

   MarshallableEntryImpl(K key, V value, Metadata metadata, PrivateMetadata internalMetadata, long created, long lastUsed, Marshaller marshaller) {
      this.key = key;
      this.value = value;
      this.metadata = metadata;
      this.internalMetadata = internalMetadata;
      this.created = created;
      this.lastUsed = lastUsed;
      this.marshaller = marshaller;
   }

   MarshallableEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, ByteBuffer internalMetadataBytes, long created, long lastUsed, Marshaller marshaller) {
      this.keyBytes = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.internalMetadataBytes = internalMetadataBytes;
      this.created = created;
      this.lastUsed = lastUsed;
      this.marshaller = marshaller;
   }

   MarshallableEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, ByteBuffer internalMetadataBytes, long created, long lastUsed, Marshaller marshaller) {
      this((ByteBuffer) null, valueBytes, metadataBytes, internalMetadataBytes, created, lastUsed, marshaller);
      this.key = key;
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
   public Metadata getMetadata() {
      if (metadata == null) {
         if (metadataBytes == null)
            return null;
         else
            metadata = unmarshall(metadataBytes);
      }
      return metadata;
   }

   @Override
   public PrivateMetadata getInternalMetadata() {
      if (internalMetadata == null) {
         if (internalMetadataBytes == null)
            return null;
         else
            internalMetadata = unmarshall(internalMetadataBytes);
      }
      return internalMetadata;
   }

   @Override
   public ByteBuffer getKeyBytes() {
      if (keyBytes == null)
         keyBytes = marshall(key, marshaller);
      return keyBytes;
   }

   @Override
   public ByteBuffer getValueBytes() {
      if (valueBytes == null)
         valueBytes = marshall(value, marshaller);
      return valueBytes;
   }

   @Override
   public ByteBuffer getMetadataBytes() {
      if (metadataBytes == null)
         metadataBytes = marshall(metadata, marshaller);
      return metadataBytes;
   }

   @Override
   public ByteBuffer getInternalMetadataBytes() {
      if (internalMetadataBytes == null)
         internalMetadataBytes = marshall(internalMetadata, marshaller);
      return internalMetadataBytes;
   }

   @Override
   public long created() {
      return created;
   }

   @Override
   public long lastUsed() {
      return lastUsed;
   }

   @Override
   public boolean isExpired(long now) {
      return isExpired(getMetadata(), now, created(), lastUsed());
   }

   public static boolean isExpired(Metadata metadata, long now, long created, long lastUsed) {
      long expiry = expiryTime(metadata, created, lastUsed);
      return expiry > 0 && expiry <= now;
   }

   @Override
   public long expiryTime() {
      return expiryTime(getMetadata(), created(), lastUsed());
   }

   public static long expiryTime(Metadata metadata, long created, long lastUsed) {
      if (metadata == null) return -1;
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = metadata.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   public MarshalledValue getMarshalledValue() {
      return new MarshalledValueImpl(getValueBytes(), getMetadataBytes(), getInternalMetadataBytes(), created, lastUsed);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MarshallableEntryImpl)) return false;

      MarshallableEntryImpl<?, ?> that = (MarshallableEntryImpl<?, ?>) o;

      return Objects.equals(getKeyBytes(), that.getKeyBytes()) &&
            Objects.equals(getMetadataBytes(), that.getMetadataBytes()) &&
            Objects.equals(getInternalMetadata(), that.getInternalMetadata()) &&
            Objects.equals(getValueBytes(), that.getValueBytes()) &&
            expiryTime() == that.expiryTime();
   }

   @Override
   public int hashCode() {
      long expiryTime = expiryTime();
      int result = getKeyBytes() != null ? getKeyBytes().hashCode() : 0;
      result = 31 * result + (getValueBytes() != null ? getValueBytes().hashCode() : 0);
      result = 31 * result + (getMetadataBytes() != null ? getMetadataBytes().hashCode() : 0);
      result = 31 * result + (getInternalMetadataBytes() != null ? getInternalMetadataBytes().hashCode() : 0);
      result = 31 * result + (int) (expiryTime ^ (expiryTime >>> 32));
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder().append(this.getClass().getSimpleName())
            .append("{keyBytes=").append(keyBytes)
            .append(", valueBytes=").append(valueBytes)
            .append(", metadataBytes=").append(metadataBytes)
            .append(", internalMetadataBytes=").append(internalMetadataBytes)
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
      sb.append(", internalMetadata=").append(internalMetadata);
      if (internalMetadata == null && internalMetadataBytes != null && marshaller != null) {
         sb.append('/').append(this.<Object>unmarshall(internalMetadataBytes));
      }
      sb.append(", created=").append(created);
      sb.append(", lastUsed=").append(lastUsed);
      sb.append(", marshaller=").append(marshaller).append('}');
      return sb.toString();
   }

   static ByteBuffer marshall(Object obj, Marshaller marshaller) {
      if (obj == null)
         return null;

      try {
         return marshaller.objectToBuffer(obj);
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }

   <T> T unmarshall(ByteBuffer buf) {
      return unmarshall(buf, marshaller);
   }

   @SuppressWarnings(value = "unchecked")
   static <T> T unmarshall(ByteBuffer buf, Marshaller marshaller) {
      if (buf == null)
         return null;

      try {
         return (T) marshaller.objectFromByteBuffer(buf.getBuf(), buf.getOffset(), buf.getLength());
      } catch (Exception e) {
         throw new PersistenceException(e);
      }
   }
}
