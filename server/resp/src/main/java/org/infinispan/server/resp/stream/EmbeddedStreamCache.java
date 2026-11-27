package org.infinispan.server.resp.stream;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.functional.FunctionalMap;

/**
 * A cache implementation for the stream data type, providing the operations utilized by the RESP stream commands.
 *
 * @since 16.4
 */
public class EmbeddedStreamCache {

   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";

   protected final FunctionalMap.ReadWriteMap<byte[], StreamBucket> readWriteMap;
   protected final AdvancedCache<byte[], StreamBucket> cache;

   public EmbeddedStreamCache(Cache<byte[], StreamBucket> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMap<byte[], StreamBucket> functionalMap = FunctionalMap.create(this.cache);
      this.readWriteMap = functionalMap.toReadWriteMap();
   }

   /**
    * Appends an entry to the stream, generating the ID when requested, and applying the trimming strategy. Returns
    * {@code null} when the stream does not exist and the {@code NOMKSTREAM} option is utilized.
    *
    * @param key The name of the stream.
    * @param function The add operation, see {@link StreamAddFunction}.
    * @return The ID of the appended entry, or {@code null} with {@code NOMKSTREAM} on a missing key.
    */
   public CompletionStage<byte[]> add(byte[] key, StreamAddFunction function) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, function);
   }

   /**
    * Retrieves the entries within the provided ID range.
    *
    * @param key The name of the stream.
    * @param function The range operation, see {@link StreamRangeFunction}.
    * @return The entries in the range, possibly empty.
    */
   public CompletionStage<List<StreamEntry>> range(byte[] key, StreamRangeFunction function) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, function);
   }

   /**
    * Retrieves the entries with an ID strictly greater than the provided one. Utilized by {@code XREAD}.
    *
    * @param key The name of the stream.
    * @param id The last ID the caller received.
    * @param count The maximum number of entries to return, or a non-positive value for no limit.
    * @return The entries with an ID greater than the one provided, possibly empty.
    */
   public CompletionStage<List<StreamEntry>> read(byte[] key, StreamID id, long count) {
      return range(key, new StreamRangeFunction(id, true, StreamID.MAX, false, false, count));
   }

   /**
    * Retrieves the last entry in the stream. Utilized by {@code XREAD} with the special {@code +} ID.
    *
    * @param key The name of the stream.
    * @return The last entry, or an empty list when the stream does not exist or is empty.
    */
   public CompletionStage<List<StreamEntry>> readLast(byte[] key) {
      return range(key, new StreamRangeFunction(StreamID.ZERO, false, StreamID.MAX, false, true, 1));
   }

   /**
    * Removes the entries matching the provided IDs.
    *
    * @param key The name of the stream.
    * @param ids The IDs to remove.
    * @return The number of entries removed.
    */
   public CompletionStage<Long> delete(byte[] key, List<StreamID> ids) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new StreamDeleteFunction(ids));
   }

   /**
    * Trims the stream evicting the oldest entries according to the trimming strategy.
    *
    * @param key The name of the stream.
    * @param function The trim operation, see {@link StreamTrimFunction}.
    * @return The number of entries evicted.
    */
   public CompletionStage<Long> trim(byte[] key, StreamTrimFunction function) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, function);
   }

   /**
    * Sets the last generated ID of an existing stream.
    *
    * @param key The name of the stream.
    * @param function The set ID operation, see {@link StreamSetIdFunction}.
    * @return {@code true} when the ID was set.
    */
   public CompletionStage<Boolean> setId(byte[] key, StreamSetIdFunction function) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, function);
   }

   /**
    * Retrieves the number of entries in the stream.
    *
    * @param key The name of the stream.
    * @return The number of entries, or zero when the key does not exist.
    */
   public CompletionStage<Long> size(byte[] key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> b == null ? 0L : (long) b.size());
   }

   /**
    * Retrieves the bucket storing the stream. Utilized by {@code XINFO STREAM}.
    *
    * @param key The name of the stream.
    * @return The stream bucket, or {@code null} when the key does not exist.
    */
   public CompletionStage<StreamBucket> bucket(byte[] key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key);
   }
}
