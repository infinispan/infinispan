package org.infinispan.api.mutiny;

import java.nio.ByteBuffer;
import java.util.List;

import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.sync.SyncCache;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * SyncStreamingCache implements streaming versions of most {@link SyncCache} methods
 *
 * @since 14.0
 */
public interface MutinyStreamingCache<K> {
   /**
    * Retrieves the value of the specified key as an {@link CacheEntrySubscriber}. The marshaller is ignored, i.e. all data will be
    * read in its raw binary form.
    *
    * @param key                  key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   default CacheEntrySubscriber get(K key, BackPressureStrategy backPressureStrategy) {
      return get(key, backPressureStrategy, CacheOptions.DEFAULT);
   }

   /**
    * Retrieves the value of the specified key as an {@link CacheEntrySubscriber}. The marshaller is ignored, i.e. all data will be
    * read in its raw binary form.
    *
    * @param key                  key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    * @param options
    */
   CacheEntrySubscriber get(K key, BackPressureStrategy backPressureStrategy, CacheOptions options);

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link MultiEmitter} and
    * complete it when there is no more data to write. The marshaller is ignored, i.e. all data will be written in its
    * raw binary form. The returned {@link MultiEmitter} is not thread-safe.
    *
    * @param key                  key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   default CacheEntryPublisher put(K key, BackPressureStrategy backPressureStrategy) {
      return put(key, CacheWriteOptions.DEFAULT, backPressureStrategy);
   }

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link MultiEmitter} and
    * complete it when there is no more data to write. The marshaller is ignored, i.e. all data will be written in its
    * raw binary form. The returned {@link MultiEmitter} is not thread-safe.
    *
    * @param key                  key to use
    * @param options
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   CacheEntryPublisher put(K key, CacheWriteOptions options, BackPressureStrategy backPressureStrategy);

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the {@link MultiEmitter} has been completed.
    * The returned {@link MultiEmitter} is not thread-safe.
    *
    * @param key                  key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   default CacheEntryPublisher putIfAbsent(K key, BackPressureStrategy backPressureStrategy) {
      return putIfAbsent(key, CacheWriteOptions.DEFAULT, backPressureStrategy);
   }

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the {@link MultiEmitter} has been completed.
    * The returned {@link MultiEmitter} is not thread-safe.
    *
    * @param key                  key to use
    * @param options              the entry expiration
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   CacheEntryPublisher putIfAbsent(K key, CacheWriteOptions options, BackPressureStrategy backPressureStrategy);


   interface CacheEntrySubscriber extends Multi<List<ByteBuffer>> {
      Uni<CacheEntryMetadata> metadata();
   }

   interface CacheEntryPublisher extends Multi<ByteBuffer>, AutoCloseable {
   }
}
