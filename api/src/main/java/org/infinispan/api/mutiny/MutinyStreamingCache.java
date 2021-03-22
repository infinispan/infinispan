package org.infinispan.api.mutiny;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.sync.SyncCache;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.tuples.Tuple2;

/**
 * SyncStreamingCache implements streaming versions of most {@link SyncCache} methods
 *
 * @since 14.0
 */
public interface MutinyStreamingCache<K> {
   /**
    * Retrieves the value of the specified key as an {@link Multi} of {@link ByteBuffer}.
    * The marshaller is ignored, i.e. all data will be read in its raw binary form.
    * The returned tuple also includes the {@link CacheEntryMetadata} interface.
    * The returned input stream is not thread-safe.
    *
    * @param key      key to use
    * @param backPressureStrategy the {@link BackPressureStrategy} to apply
    */
   Tuple2<Uni<CacheEntryMetadata>, Multi<ByteBuffer>> get(K key, BackPressureStrategy backPressureStrategy);

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
}
