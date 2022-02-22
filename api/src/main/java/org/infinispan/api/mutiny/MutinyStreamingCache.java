package org.infinispan.api.mutiny;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.infinispan.api.common.CacheEntryMetadata;
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
    * Retrieves the value of the specified key as an {@link Value}.
    * The marshaller is ignored, i.e. all data will be read in its raw binary form.
    * The returned input stream is not thread-safe.
    *
    * @param key      key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
    Value get(K key, BackPressureStrategy backPressureStrategy);

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link OutputStream}
    * and close it when there is no more data to write. The marshaller is ignored, i.e. all data will be written in its
    * raw binary form. The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   MultiEmitter<ByteBuffer> put(K key, BackPressureStrategy backPressureStrategy);

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the stream has been closed.
    * The returned output stream is not thread-safe.
    *
    * @param key     key to use
    */
   OutputStream putIfAbsent(K key);

   /**
    * A streaming value.
    */
   interface Value {
      Uni<CacheEntryMetadata> metadata();

      Multi<ByteBuffer> value();
   }
}
