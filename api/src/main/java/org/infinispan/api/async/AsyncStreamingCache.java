package org.infinispan.api.async;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;

/**
 * AsyncStreamingCache implements streaming versions of put and get methods
 *
 * @since 14.0
 */
public interface AsyncStreamingCache<K> {
   /**
    * Retrieves the value of the specified key as a {@link CacheEntrySubscriber}. It is up to the application to ensure
    * that the data is consumed and closed. The marshaller is ignored, i.e. all data will be read in its raw binary
    * form.
    *
    * @param key key to use
    */
   default CacheEntrySubscriber get(K key) {
      return get(key, CacheOptions.DEFAULT);
   }

   /**
    * Retrieves the value of the specified key as a {@link CacheEntrySubscriber}. It is up to the application to ensure
    * that the data is consumed and closed. The marshaller is ignored, i.e. all data will be read in its raw binary
    * form.
    *
    * @param key      key to use
    * @param metadata
    */
   CacheEntrySubscriber get(K key, CacheOptions metadata);

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link
    * CacheEntryPublisher} and close it when there is no more data to write. The marshaller is ignored, i.e. all data
    * will be written in its raw binary form. The returned output stream is not thread-safe.
    *
    * @param key key to use
    */
   default CacheEntryPublisher put(K key) {
      return put(key, CacheWriteOptions.DEFAULT);
   }

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link
    * CacheEntryPublisher} and close it when there is no more data to write. The marshaller is ignored, i.e. all data
    * will be written in its raw binary form. The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param metadata metadata
    */
   CacheEntryPublisher put(K key, CacheWriteOptions metadata);

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the data is complete.
    *
    * @param key key to use
    */
   default CacheEntryPublisher putIfAbsent(K key) {
      return putIfAbsent(key, CacheWriteOptions.DEFAULT);
   }

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the data is complete.
    *
    * @param key      key to use
    * @param metadata metadata
    */
   CacheEntryPublisher putIfAbsent(K key, CacheWriteOptions metadata);

   interface CacheEntrySubscriber extends Flow.Subscriber<List<ByteBuffer>> {
      CompletionStage<CacheEntryMetadata> metadata();
   }

   interface CacheEntryPublisher extends Flow.Publisher<ByteBuffer>, AutoCloseable {
   }
}
