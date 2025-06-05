package org.infinispan.jcache.embedded;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.cache.integration.CacheWriter;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.encoding.DataConversion;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.JCacheEntry;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.concurrent.BlockingManager;

public class JCacheWriterAdapter<K, V> implements NonBlockingStore<K, V> {

   private CacheWriter<? super K, ? super V> delegate;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;
   private BlockingManager blockingManager;

   public JCacheWriterAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
      this.keyDataConversion = DataConversion.IDENTITY_KEY;
      this.valueDataConversion = DataConversion.IDENTITY_KEY;
   }

   public void setCacheWriter(CacheWriter<? super K, ? super V> delegate) {
      this.delegate = delegate;
   }

   public void setDataConversion(DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Override
   public Set<Characteristic> characteristics() {
      return EnumSet.of(Characteristic.WRITE_ONLY, Characteristic.SEGMENTABLE);
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      this.blockingManager = ctx.getBlockingManager();
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> stop() {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return blockingManager.runBlocking(() -> {
         try {
            delegate.write(new JCacheEntry(keyDataConversion.fromStorage(entry.getKey()), valueDataConversion.fromStorage(entry.getValue())));
         } catch (Exception e) {
            throw Exceptions.launderCacheWriterException(e);
         }
      }, "jcache-write");
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return blockingManager.supplyBlocking(() -> {
         try {
            delegate.delete(keyDataConversion.fromStorage(key));
         } catch (Exception e) {
            throw Exceptions.launderCacheWriterException(e);
         }
         return false;
      }, "jcache-delete");
   }

   @Override
   public CompletionStage<Void> clear() {
      return CompletableFutures.completedNull();
   }
}
