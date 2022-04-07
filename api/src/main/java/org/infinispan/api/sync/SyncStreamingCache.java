package org.infinispan.api.sync;

import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;

/**
 * SyncStreamingCache implements streaming versions of most {@link SyncCache} methods
 *
 * @since 14.0
 */
public interface SyncStreamingCache<K> {
   /**
    * Retrieves the value of the specified key as an {@link InputStream}. It is up to the application to ensure that the
    * stream is consumed and closed. The marshaller is ignored, i.e. all data will be read in its raw binary form. The
    * returned input stream implements the {@link CacheEntryMetadata} interface. The returned input stream is not
    * thread-safe.
    *
    * @param key key to use
    */
   default <T extends InputStream & CacheEntryMetadata> T get(K key) {
      return get(key, CacheOptions.DEFAULT);
   }

   /**
    * Retrieves the value of the specified key as an {@link InputStream}. It is up to the application to ensure that the
    * stream is consumed and closed. The marshaller is ignored, i.e. all data will be read in its raw binary form. The
    * returned input stream implements the {@link CacheEntryMetadata} interface. The returned input stream is not
    * thread-safe.
    *
    * @param key key to use
    */
   <T extends InputStream & CacheEntryMetadata> T get(K key, CacheOptions options);

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link OutputStream} and
    * close it when there is no more data to write. The marshaller is ignored, i.e. all data will be written in its raw
    * binary form. The returned output stream is not thread-safe.
    *
    * @param key key to use
    */
   default OutputStream put(K key) {
      return put(key, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param options
    * @return
    */
   OutputStream put(K key, CacheOptions options);

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the stream has been closed. The returned
    * output stream is not thread-safe.
    *
    * @param key key to use
    */
   default OutputStream putIfAbsent(K key) {
      return putIfAbsent(key, CacheWriteOptions.DEFAULT);
   }

   /**
    * @param key
    * @param options
    * @return
    */
   OutputStream putIfAbsent(K key, CacheWriteOptions options);
}
