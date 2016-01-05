package org.infinispan.persistence.remote.upgrade;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.protocol.VersionUtils;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gustavonalle
 * @since 8.2
 */
public class HotRodMigratorHelper {

   static final String MIGRATION_MANAGER_HOT_ROD_KNOWN_KEYS = "___MigrationManager_HotRod_KnownKeys___";
   static final String ITERATOR_MINIMUM_VERSION = "2.5";
   static final int DEFAULT_READ_BATCH_SIZE = 10000;

   private static final Log log = LogFactory.getLog(HotRodMigratorHelper.class, Log.class);

   static boolean supportsIteration(String protocolVersion) {
      return protocolVersion == null || VersionUtils.isVersionGreaterOrEquals(protocolVersion, ITERATOR_MINIMUM_VERSION);
   }

   static List<Integer> range(int end) {
      List<Integer> integers = new ArrayList<>();
      for (int i = 0; i < end; i++) {
         integers.add(i);
      }
      return integers;
   }

   static <T> List<List<T>> split(List<T> list, final int parts) {
      List<List<T>> subLists = new ArrayList<>(parts);
      for (int i = 0; i < parts; i++) {
         subLists.add(new ArrayList<T>());
      }
      for (int i = 0; i < list.size(); i++) {
         subLists.get(i % parts).add(list.get(i));
      }
      return subLists;
   }

   static void gracefulShutdown(ExecutorService executorService) {
      try {
         executorService.shutdown();
         while (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) ;
      } catch (InterruptedException e) {
         throw new CacheException(e);
      }
   }

   static void migrateEntriesWithMetadata(RemoteCache<Object, Object> sourceCache, Cache<Object, Object> destCache,
                                          ExecutorService executorService, byte[] ignoreKey, AtomicInteger counter,
                                          Set<Integer> segments, int readBatch) {
      try (CloseableIterator<Map.Entry<Object, MetadataValue<Object>>> iterator = sourceCache.retrieveEntriesWithMetadata(segments, readBatch)) {
         while (iterator.hasNext()) {
            Map.Entry<Object, MetadataValue<Object>> entry = iterator.next();
            if (!Arrays.equals((byte[]) entry.getKey(), ignoreKey)) {
               MetadataValue<Object> metadataValue = entry.getValue();
               int lifespan = metadataValue.getLifespan();
               int maxIdle = metadataValue.getMaxIdle();
               long version = metadataValue.getVersion();
               long created = metadataValue.getCreated();
               long lastUsed = metadataValue.getLastUsed();
               Metadata metadata = new EmbeddedMetadata.Builder()
                       .version(new NumericVersion(version))
                       .lifespan(lifespan, TimeUnit.SECONDS)
                       .maxIdle(maxIdle, TimeUnit.SECONDS)
                       .build();
               InternalMetadataImpl internalMetadata = new InternalMetadataImpl(metadata, created, lastUsed);
               executorService.submit(() -> {
                  destCache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put(entry.getKey(), entry.getValue().getValue(), internalMetadata);
                  int currentCount = counter.incrementAndGet();
                  if (log.isDebugEnabled() && currentCount % 100 == 0)
                     log.debugf(">>    Migrated %s entries\n", currentCount);
               });

            }
         }
      }
   }

}
