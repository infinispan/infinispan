package org.infinispan.marshall.persistence.impl;

import static java.lang.Math.min;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
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
   volatile ByteBuffer keyBytes;
   volatile transient K key;
   volatile transient V value;
   volatile transient Metadata metadata;
   transient org.infinispan.commons.marshall.Marshaller marshaller;

   MarshallableEntryImpl() {}

   MarshallableEntryImpl(K key, V value, Metadata metadata, long created, long lastUsed, Marshaller marshaller) {
      this.key = key;
      this.value = value;
      this.metadata = metadata;
      this.keyBytes = marshall(key, marshaller);
      this.valueBytes = marshall(value, marshaller);
      this.metadataBytes = marshall(metadata, marshaller);
      this.created = created;
      this.lastUsed = lastUsed;
      this.marshaller = marshaller;
   }

   MarshallableEntryImpl(ByteBuffer key, ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed, Marshaller marshaller) {
      this.keyBytes = key;
      this.valueBytes = valueBytes;
      this.metadataBytes = metadataBytes;
      this.created = created;
      this.lastUsed = lastUsed;
      this.marshaller = marshaller;
   }

   MarshallableEntryImpl(K key, ByteBuffer valueBytes, ByteBuffer metadataBytes, long created, long lastUsed, Marshaller marshaller) {
      this(marshall(key, marshaller), valueBytes, metadataBytes, created, lastUsed, marshaller);
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
   public ByteBuffer getKeyBytes() {
      if (keyBytes == null)
         keyBytes = marshall(key, marshaller);
      return keyBytes;
   }

   @Override
   public ByteBuffer getValueBytes() {
      return valueBytes;
   }

   @Override
   public ByteBuffer getMetadataBytes() {
      return metadataBytes;
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
      long expiry = expiryTime();
      return expiry > 0 && expiry <= now;
   }

   @Override
   public long expiryTime() {
      Metadata metadata = getMetadata();
      if (metadata == null) return -1;
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created() + lifespan : -1;
      long maxIdle = metadata.maxIdle();
      long muet = maxIdle > -1 ? lastUsed() + maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   public MarshalledEntry<K, V> asMarshalledEntry() {
      Metadata meta = getMetadata();
      InternalMetadataImpl internalMeta = meta == null ? null : new InternalMetadataImpl(meta, created(), lastUsed());
      return new MarshalledEntryImpl<>(getKey(), getValue(), internalMeta, marshaller);
   }

   @Override
   public MarshalledValue getMarshalledValue() {
      return new MarshalledValueImpl(valueBytes, metadataBytes, created, lastUsed);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MarshallableEntryImpl)) return false;

      MarshallableEntryImpl that = (MarshallableEntryImpl) o;

      if (getKeyBytes() != null ? !getKeyBytes().equals(that.getKeyBytes()) : that.getKeyBytes() != null) return false;
      if (getMetadataBytes() != null ? !getMetadataBytes().equals(that.getMetadataBytes()) : that.getMetadataBytes() != null) return false;
      if (getValueBytes() != null ? !getValueBytes().equals(that.getValueBytes()) : that.getValueBytes() != null) return false;
      if (expiryTime() != that.expiryTime()) return false;
      return true;
   }

   @Override
   public int hashCode() {
      long expiryTime = expiryTime();
      int result = getKeyBytes() != null ? getKeyBytes().hashCode() : 0;
      result = 31 * result + (getValueBytes() != null ? getValueBytes().hashCode() : 0);
      result = 31 * result + (getMetadataBytes() != null ? getMetadataBytes().hashCode() : 0);
      result = 31 * result + (int) (expiryTime ^ (expiryTime >>> 32));
      return result;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder().append(this.getClass().getSimpleName())
            .append("{keyBytes=").append(keyBytes)
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
