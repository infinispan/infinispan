package org.infinispan.server.cachemigrator;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.multimap.impl.HashMapBucket;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.multimap.impl.SetBucket;
import org.infinispan.multimap.impl.SortedSetBucket;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Service for migrating cache entries from one cache to another.
 *
 * @author Infinispan
 * @since 16.2
 */
public class CacheMigrationService {

   private static final Log log = LogFactory.getLog(CacheMigrationService.class);

   private final RemoteCacheManager cacheManager;
   private final int batchSize;
   private final int concurrency;

   public CacheMigrationService(RemoteCacheManager cacheManager) {
      this(cacheManager, 1000, 16);
   }

   public CacheMigrationService(RemoteCacheManager cacheManager, int batchSize, int concurrency) {
      this.cacheManager = cacheManager;
      this.batchSize = batchSize;
      this.concurrency = concurrency;
   }

   /**
    * Create a cache with the specified configuration. If no configuration is provided,
    * uses a default configuration with octet-stream keys and x-java-object values.
    * The configuration string can be in XML, JSON, or YAML format - the server will
    * detect the format automatically. The user-provided configuration will be read on
    * top of the defaults, allowing selective overrides.
    *
    * @param cacheName the name of the cache to create
    * @param configuration the cache configuration in XML, JSON, or YAML format (can be null for defaults)
    * @return the created cache
    */
   public <K, V> RemoteCache<K, V> createTargetCache(String cacheName, String configuration) {
      Configuration config = buildConfiguration(configuration);
      log.debugf("Creating cache '%s' with %s configuration", cacheName,
            configuration != null ? "merged" : "default");
      StringConfiguration stringConfig = new StringConfiguration(config.toStringConfiguration(cacheName));
      return cacheManager.administration().createCache(cacheName, stringConfig);
   }

   /**
    * Create a cache with the specified configuration if it doesn't already exist.
    * If no configuration is provided, uses a default configuration with octet-stream
    * keys and x-java-object values. The configuration string can be in XML, JSON,
    * or YAML format - the server will detect the format automatically. The user-provided
    * configuration will be read on top of the defaults, allowing selective overrides.
    *
    * @param cacheName the name of the cache to create
    * @param configuration the cache configuration in XML, JSON, or YAML format (can be null for defaults)
    * @return the cache (either newly created or existing)
    */
   public <K, V> RemoteCache<K, V> getOrCreateTargetCache(String cacheName, String configuration) {
      Configuration config = buildConfiguration(configuration);
      log.debugf("Getting or creating cache '%s' with %s configuration", cacheName,
            configuration != null ? "merged" : "default");
      StringConfiguration stringConfig = new StringConfiguration(config.toStringConfiguration(cacheName));
      return cacheManager.administration().getOrCreateCache(cacheName, stringConfig);
   }

   /**
    * Builds a cache configuration by starting with defaults and optionally reading
    * the provided configuration builder on top.
    *
    * @param userBuilder the user configuration builder to merge with defaults (can be null)
    * @return the resulting Configuration
    */
   public Configuration buildConfiguration(ConfigurationBuilder userBuilder) {
      // Start with default configuration
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(org.infinispan.configuration.cache.CacheMode.DIST_SYNC);
      builder.encoding()
            .key().mediaType(MediaType.APPLICATION_OCTET_STREAM_TYPE)
            .encoding()
            .value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);

      // If user provided configuration, read it on top of defaults
      if (userBuilder != null) {
         builder.read(userBuilder.build());
      }

      return builder.build();
   }

   /**
    * Builds a cache configuration by starting with defaults and optionally reading
    * user-provided configuration on top.
    *
    * @param userConfiguration the user configuration string (XML, JSON, or YAML), can be null
    * @return the resulting Configuration
    */
   private Configuration buildConfiguration(String userConfiguration) {
      ConfigurationBuilder userBuilder = null;

      // If user provided configuration, parse it
      if (userConfiguration != null && !userConfiguration.isEmpty()) {
         try {
            ParserRegistry parserRegistry = new ParserRegistry();
            ConfigurationBuilderHolder holder = parserRegistry.parse(userConfiguration, null);
            userBuilder = holder.getCurrentConfigurationBuilder();
         } catch (Exception e) {
            log.warnf(e, "Failed to parse user configuration, using defaults: %s", e.getMessage());
         }
      }

      return buildConfiguration(userBuilder);
   }

   /**
    * Migrate all entries from source cache to target cache using old RESP encoding.
    * <p>
    * <strong>IMPORTANT:</strong> This method should only be used when migrating caches that were
    * configured to be used for the RESP endpoint and are running on an Infinispan server version
    * <strong>before 16.2</strong>. Starting with version 16.2, RESP encoding was changed, and this
    * migration mode helps transition from the old encoding to the new one.
    * <p>
    * Attempts to deserialize values using the global marshaller. If the value is a RESP object
    * type (HashMapBucket, ListBucket, SetBucket, SortedSetBucket, byte[], JsonBucket), the
    * deserialized object is used. Otherwise, the raw byte[] is used.
    *
    * @param sourceCacheName the name of the source cache
    * @param targetCacheName the name of the target cache
    * @return migration result containing statistics
    */
   public MigrationResult migrateOldResp(String sourceCacheName, String targetCacheName) {
      return migrateOldResp(sourceCacheName, targetCacheName, null);
   }

   /**
    * Migrate all entries from source cache to target cache using old RESP encoding.
    * <p>
    * <strong>IMPORTANT:</strong> This method should only be used when migrating caches that were
    * configured to be used for the RESP endpoint and are running on an Infinispan server version
    * <strong>before 16.2</strong>. Starting with version 16.2, RESP encoding was changed, and this
    * migration mode helps transition from the old encoding to the new one.
    * <p>
    * Attempts to deserialize values using the global marshaller. If the value is a RESP object
    * type (HashMapBucket, ListBucket, SetBucket, SortedSetBucket, byte[], JsonBucket), the
    * deserialized object is used. Otherwise, the raw byte[] is used.
    *
    * @param sourceCacheName the name of the source cache
    * @param targetCacheName the name of the target cache
    * @param progressCallback optional callback for progress updates (receives count of processed entries)
    * @return migration result containing statistics
    */
   public MigrationResult migrateOldResp(String sourceCacheName, String targetCacheName,
                                          Consumer<Long> progressCallback) {
      RemoteCache<byte[], byte[]> sourceCache = cacheManager.getCache(sourceCacheName)
            .withDataFormat(DataFormat.builder()
                  .keyType(MediaType.APPLICATION_OCTET_STREAM)
                  .valueType(MediaType.APPLICATION_OCTET_STREAM)
                  .build());

      RemoteCache<byte[], Object> targetCache = cacheManager.getCache(targetCacheName)
            .withDataFormat(DataFormat.builder()
                  .keyType(MediaType.APPLICATION_OCTET_STREAM)
                  .valueType(MediaType.APPLICATION_OBJECT)
                  .build());

      Marshaller marshaller = cacheManager.getMarshaller();

      log.debugf("Starting old RESP migration from cache '%s' to cache '%s' with batchSize=%d, concurrency=%d",
            sourceCache.getName(), targetCache.getName(), batchSize, concurrency);

      return migrateInternal(sourceCache, targetCache, entry -> {
         try {
            Object deserialized = marshaller.objectFromByteBuffer(entry.getValue());
            // Check if it's a RESP type
            if (isRespType(deserialized)) {
               log.tracef("Deserialized value as %s", deserialized.getClass().getName());
               return deserialized;
            } else {
               log.tracef("Value type %s is not a RESP type, using raw bytes", deserialized.getClass().getName());
               return entry.getValue();
            }
         } catch (Exception e) {
            // If deserialization fails, use raw bytes
            log.tracef(e, "Failed to deserialize value, using raw bytes");
            return entry.getValue();
         }
      }, progressCallback);
   }

   /**
    * Check if an object is a RESP type that should be deserialized.
    * Checks for: HashMapBucket, ListBucket, SetBucket, SortedSetBucket, byte[], and JsonBucket.
    * JsonBucket is checked by class name to avoid circular dependency with server-resp.
    */
   private boolean isRespType(Object obj) {
      if (obj == null) {
         return false;
      }
      String className = obj.getClass().getName();
      return obj instanceof HashMapBucket
            || obj instanceof ListBucket
            || obj instanceof SetBucket
            || obj instanceof SortedSetBucket
            || obj instanceof byte[]
            || "org.infinispan.server.resp.json.JsonBucket".equals(className);
   }

   /**
    * Migrate all entries from source cache to target cache using octet-stream data format.
    *
    * @param sourceCacheName the name of the source cache
    * @param targetCacheName the name of the target cache
    * @return migration result containing statistics
    */
   public MigrationResult migrate(String sourceCacheName, String targetCacheName) {
      return migrate(sourceCacheName, targetCacheName, null);
   }

   /**
    * Migrate all entries from source cache to target cache using octet-stream data format.
    *
    * @param sourceCacheName the name of the source cache
    * @param targetCacheName the name of the target cache
    * @param progressCallback optional callback for progress updates (receives count of processed entries)
    * @return migration result containing statistics
    */
   public MigrationResult migrate(String sourceCacheName, String targetCacheName,
                                   Consumer<Long> progressCallback) {
      RemoteCache<byte[], byte[]> sourceCache = cacheManager.getCache(sourceCacheName)
            .withDataFormat(DataFormat.builder()
                  .keyType(MediaType.APPLICATION_OCTET_STREAM)
                  .valueType(MediaType.APPLICATION_OCTET_STREAM)
                  .build());

      RemoteCache<byte[], byte[]> targetCache = cacheManager.getCache(targetCacheName)
            .withDataFormat(DataFormat.builder()
                  .keyType(MediaType.APPLICATION_OCTET_STREAM)
                  .valueType(MediaType.APPLICATION_OCTET_STREAM)
                  .build());

      log.debugf("Starting migration from cache '%s' to cache '%s' with batchSize=%d, concurrency=%d",
            sourceCache.getName(), targetCache.getName(), batchSize, concurrency);

      return migrateInternal(sourceCache, targetCache, entry -> entry.getValue(), progressCallback);
   }

   /**
    * Migrate all entries from source cache to target cache.
    *
    * @param sourceCache the source cache
    * @param targetCache the target cache
    * @param <K> the key type
    * @param <V> the value type
    * @return migration result containing statistics
    */
   public <K, V> MigrationResult migrateCache(RemoteCache<K, V> sourceCache,
                                               RemoteCache<K, V> targetCache) {
      return migrateCache(sourceCache, targetCache, null);
   }

   /**
    * Migrate all entries from source cache to target cache.
    *
    * @param sourceCache the source cache
    * @param targetCache the target cache
    * @param progressCallback optional callback for progress updates (receives count of processed entries)
    * @param <K> the key type
    * @param <V> the value type
    * @return migration result containing statistics
    */
   public <K, V> MigrationResult migrateCache(RemoteCache<K, V> sourceCache,
                                               RemoteCache<K, V> targetCache,
                                               Consumer<Long> progressCallback) {
      log.debugf("Starting migration from cache '%s' to cache '%s' with batchSize=%d, concurrency=%d",
            sourceCache.getName(), targetCache.getName(), batchSize, concurrency);

      return migrateInternal(sourceCache, targetCache, entry -> entry.getValue(), progressCallback);
   }

   /**
    * Internal method that performs the actual migration using reactive streams.
    *
    * @param sourceCache the source cache to read from
    * @param targetCache the target cache to write to
    * @param valueTransformer function to transform values before writing (e.g., deserialize for old RESP)
    * @param progressCallback optional callback for progress updates
    * @param <K> the key type
    * @param <V> the source value type
    * @param <T> the target value type
    * @return migration result containing statistics
    */
   private <K, V, T> MigrationResult migrateInternal(RemoteCache<K, V> sourceCache,
                                                      RemoteCache<K, T> targetCache,
                                                      Function<Map.Entry<K, V>, T> valueTransformer,
                                                      Consumer<Long> progressCallback) {
      AtomicLong entriesProcessed = new AtomicLong(0);
      AtomicLong entriesFailed = new AtomicLong(0);

      long startTime = System.currentTimeMillis();

      Flowable<Map.Entry<K, V>> entryFlowable = Flowable.fromPublisher(
            sourceCache.publishEntries(null, null, null, batchSize));

      entryFlowable.flatMapCompletable(entry -> {
               T value = valueTransformer.apply(entry);
               CompletableFuture<Void> future = targetCache.putAsync(entry.getKey(), value)
                     .thenAccept(v -> {
                        long count = entriesProcessed.incrementAndGet();
                        if (progressCallback != null) {
                           progressCallback.accept(count);
                        }
                     })
                     .exceptionally(throwable -> {
                        long failedCount = entriesFailed.incrementAndGet();
                        log.warnf(throwable, "Failed to migrate entry with key %s (total failures: %d)",
                              Util.toStr(entry.getKey()), failedCount);
                        return null;
                     });

               return Flowable.fromCompletionStage(future).ignoreElements();
            }, false, concurrency)
            .blockingAwait();

      long duration = System.currentTimeMillis() - startTime;

      log.debugf("Migration completed: %d entries processed, %d failed, %d ms duration",
            entriesProcessed.get(), entriesFailed.get(), duration);

      return new MigrationResult(entriesProcessed.get(), entriesFailed.get(), duration);
   }

   /**
       * Result of a cache migration operation.
       */
      public record MigrationResult(long entriesProcessed, long entriesFailed, long durationMs) {

      public long getEntriesSuccessful() {
            return entriesProcessed - entriesFailed;
         }

         public long getThroughput() {
            return durationMs > 0 ? (entriesProcessed * 1000 / durationMs) : 0;
         }

         @Override
         public String toString() {
            return "MigrationResult{" +
                  "entriesProcessed=" + entriesProcessed +
                  ", entriesFailed=" + entriesFailed +
                  ", entriesSuccessful=" + getEntriesSuccessful() +
                  ", durationMs=" + durationMs +
                  ", throughput=" + getThroughput() + " entries/sec" +
                  '}';
         }
      }
}
