package org.infinispan.hotrod.impl.transaction.entry;

import static org.infinispan.commons.util.Util.toStr;

import java.time.Duration;
import java.util.function.Function;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.impl.cache.MetadataValue;
import org.infinispan.hotrod.impl.transaction.TransactionContext;

/**
 * An entry in the {@link TransactionContext}.
 * <p>
 * It represents a single key and contains its initial version (if it was read) and the most up-to-date value (can be
 * null if the key was removed).
 *
 * @since 14.0
 */
public class TransactionEntry<K, V> {
   private final K key;

   private final long version; //version read. never changes during the transaction
   private final byte readControl;
   private V value; //null == removed
   private CacheEntryMetadata metadata;
   private boolean modified;

   private TransactionEntry(K key, long version, byte readControl) {
      this.key = key;
      this.version = version;
      this.readControl = readControl;
      this.modified = false;
   }

   public static <K, V> TransactionEntry<K, V> nonExistingEntry(K key) {
      return new TransactionEntry<>(key, 0, ControlByte.NON_EXISTING.bit());
   }

   public static <K, V> TransactionEntry<K, V> notReadEntry(K key) {
      return new TransactionEntry<>(key, 0, ControlByte.NOT_READ.bit());
   }

   public static <K, V> TransactionEntry<K, V> read(K key, MetadataValue<V> value) {
      TransactionEntry<K, V> txEntry = new TransactionEntry<>(key, value.getVersion(), (byte) 0);
      txEntry.value = value.getValue();
      CacheEntryExpiration expiration;
      if (value.getLifespan() < 0) {
         if (value.getMaxIdle() < 0) {
            expiration = CacheEntryExpiration.IMMORTAL;
         } else {
            expiration = CacheEntryExpiration.withMaxIdle(Duration.ofSeconds(value.getMaxIdle()));
         }
      } else {
         if (value.getMaxIdle() < 0) {
            expiration = CacheEntryExpiration.withLifespan(Duration.ofSeconds(value.getLifespan()));
         } else {
            expiration = CacheEntryExpiration.withLifespanAndMaxIdle(Duration.ofSeconds(value.getLifespan()), Duration.ofSeconds(value.getMaxIdle()));
         }
      }
      txEntry.metadata = new CacheEntryMetadataImpl(value.getCreated(), value.getLastUsed(), expiration, new CacheEntryVersionImpl(value.getVersion()));
      return txEntry;
   }

   public long getVersion() {
      return version;
   }

   public V getValue() {
      return value;
   }

   public boolean isModified() {
      return modified;
   }

   public boolean isNonExists() {
      return value == null;
   }

   public boolean exists() {
      return value != null;
   }

   public void set(CacheEntry<K, V> entry) {
      this.value = entry.value();
      this.metadata = entry.metadata();
      this.modified = true;
   }

   public void remove() {
      this.value = null;
      this.modified = true;
   }

   public Modification toModification(Function<K, byte[]> keyMarshaller, Function<V, byte[]> valueMarshaller) {
      if (value == null) {
         //remove operation
         return new Modification(keyMarshaller.apply(key), null, version, metadata.expiration(), ControlByte.REMOVE_OP.set(readControl));
      } else {
         return new Modification(keyMarshaller.apply(key), valueMarshaller.apply(value), version, metadata.expiration(), readControl);
      }
   }

   @Override
   public String toString() {
      return "TransactionEntry{" +
            "key=" + toStr(key) +
            ", version=" + version +
            ", readControl=" + ControlByte.prettyPrint(readControl) +
            ", value=" + toStr(value) +
            ", expiration=" + metadata.expiration() +
            ", modified=" + modified +
            '}';
   }
}
