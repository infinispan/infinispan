package org.infinispan.jcache.embedded;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.encoding.DataConversion;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.Expiration;
import org.infinispan.marshall.persistence.impl.MarshallableEntryImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshalledValue;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.concurrent.BlockingManager;

public class JCacheLoaderAdapter<K, V> implements NonBlockingStore<K, V> {

   private CacheLoader<K, V> delegate;
   private InitializationContext ctx;
   private ExpiryPolicy expiryPolicy;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;
   private BlockingManager blockingManager;

   public JCacheLoaderAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
      this.keyDataConversion = DataConversion.IDENTITY_KEY;
      this.valueDataConversion = DataConversion.IDENTITY_KEY;
   }

   public void setCacheLoader(CacheLoader<K, V> delegate) {
      this.delegate = delegate;
   }

   public void setExpiryPolicy(ExpiryPolicy expiryPolicy) {
      this.expiryPolicy = expiryPolicy;
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.ctx = ctx;
      this.blockingManager = ctx.getBlockingManager();
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> stop() {
      return CompletableFutures.completedNull();
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.READ_ONLY, Characteristic.SEGMENTABLE);
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return blockingManager.supplyBlocking(() -> {
         V value = loadValue(keyDataConversion.fromStorage(key));
         if (value != null) {
            Duration expiry = Expiration.getExpiry(expiryPolicy, Expiration.Operation.CREATION);
            if (expiry == null || expiry.isEternal()) {
               return new NoBytesMarshallableEntry(key, valueDataConversion.toStorage(value));
            } else {
               long now = ctx.getTimeService().wallClockTime();
               long exp = now + expiry.getTimeUnit().toMillis(expiry.getDurationAmount());
               Metadata meta = new EmbeddedMetadata.Builder().lifespan(exp - now).build();
               return new NoBytesMarshallableEntry(key, valueDataConversion.toStorage(value), meta, null, now, -1);
            }
         }
         return null;
      }, "jcache-loade");
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return CompletableFutures.completedFalse();
   }

   @Override
   public CompletionStage<Void> clear() {
      return CompletableFutures.completedNull();
   }

   public void setDataConversion(DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @SuppressWarnings("unchecked")
   private V loadValue(Object key) {
      try {
         return delegate.load((K) key);
      } catch (Exception e) {
         throw Exceptions.launderCacheLoaderException(e);
      }
   }

   static class NoBytesMarshallableEntry<K, V> implements MarshallableEntry<K, V> {
      private final K key;
      private final V value;
      private final Metadata metadata;
      private final PrivateMetadata internalMetadata;
      private final long created;
      private final long lastUsed;

      public NoBytesMarshallableEntry(K key, V value) {
         this(key, value, null, null, -1, -1);
      }

      public NoBytesMarshallableEntry(K key, V value, Metadata metadata,
                                      PrivateMetadata internalMetadata, long created, long lastUsed) {
         this.key = key;
         this.value = value;
         this.metadata = metadata;
         this.internalMetadata = internalMetadata;
         this.created = created;
         this.lastUsed = lastUsed;
      }

      @Override
      public ByteBuffer getKeyBytes() {
         throw new UnsupportedOperationException();
      }

      @Override
      public ByteBuffer getValueBytes() {
         throw new UnsupportedOperationException();
      }

      @Override
      public ByteBuffer getMetadataBytes() {
         throw new UnsupportedOperationException();
      }

      @Override
      public ByteBuffer getInternalMetadataBytes() {
         throw new UnsupportedOperationException();
      }

      @Override
      public K getKey() {
         return key;
      }

      @Override
      public V getValue() {
         return value;
      }

      @Override
      public Metadata getMetadata() {
         return metadata;
      }

      @Override
      public PrivateMetadata getInternalMetadata() {
         return internalMetadata;
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
         return MarshallableEntryImpl.isExpired(metadata, now, created, lastUsed);
      }

      @Override
      public long expiryTime() {
         return MarshallableEntryImpl.expiryTime(metadata, created, lastUsed);
      }

      @Override
      public MarshalledValue getMarshalledValue() {
         throw new UnsupportedOperationException();
      }
   }
}
