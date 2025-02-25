package org.infinispan.query.impl.massindex;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableSet;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Mass indexer task.
 *
 * @author gustavonalle
 * @since 7.1
 */
@ProtoTypeId(ProtoStreamTypeIds.INDEX_WORKER)
public final class IndexWorker implements Function<EmbeddedCacheManager, Void> {

   private final String cacheName;
   private final Collection<Class<?>> indexedTypes;
   private final boolean skipIndex;
   private final Set<Object> keys;

   IndexWorker(String cacheName, Collection<Class<?>> indexedTypes, boolean skipIndex, Set<Object> keys) {
      this.cacheName = cacheName;
      this.indexedTypes = indexedTypes;
      this.skipIndex = skipIndex;
      this.keys = keys;
   }

   @ProtoFactory
   IndexWorker(String cacheName, Collection<Class<?>> indexedTypes, boolean skipIndex, MarshallableSet<Object> keys) {
      this(cacheName, indexedTypes, skipIndex, MarshallableSet.unwrap(keys));
   }

   @ProtoField(1)
   String getCacheName() {
      return cacheName;
   }

   @ProtoField(2)
   Collection<Class<?>> getIndexedTypes() {
      return indexedTypes;
   }

   @ProtoField(3)
   boolean isSkipIndex() {
      return skipIndex;
   }

   @ProtoField(4)
   MarshallableSet<Object> getKeys() {
      return MarshallableSet.create(keys);
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      AdvancedCache<Object, Object> cache = SecurityActions.getUnwrappedCache(embeddedCacheManager.getCache(cacheName)).getAdvancedCache();
      DataConversion valueDataConversion = cache.getValueDataConversion();

      AdvancedCache<Object, Object> reindexCache = cache.withStorageMediaType();
      boolean javaEmbeddedEntities = SecurityActions.getCacheConfiguration(reindexCache).indexing().useJavaEmbeddedEntities();

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      TimeService timeService = ComponentRegistryUtils.getTimeService(cache);

      MassIndexerProgressNotifier notifier = new MassIndexerProgressNotifier(searchMapping, timeService);
      IndexUpdater indexUpdater = new IndexUpdater(searchMapping);
      KeyPartitioner keyPartitioner = ComponentRegistryUtils.getKeyPartitioner(cache);

      if (keys == null || keys.isEmpty()) {
         preIndex(cache, indexUpdater, notifier);
         MassIndexerProgressState progressState = new MassIndexerProgressState(notifier);

         if (!skipIndex) {
            try (Stream<CacheEntry<Object, Object>> stream = reindexCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
                  .cacheEntrySet().stream()) {
               Iterator<UpdateRecord> records = stream.map(entry -> {
                        Object key = entry.getKey();
                        Object value = valueDataConversion.extractIndexable(entry.getValue(), javaEmbeddedEntities);
                        int segment = keyPartitioner.getSegment(key);

                        if (value != null && indexUpdater.typeIsIndexed(value, indexedTypes)) {
                           return new UpdateRecord(key, value, segment);
                        }
                        return new UpdateRecord(null, null, -1);
                     }).filter(record -> record.key != null)
                     .iterator();

               join(CompletionStages.performConcurrently(() -> records, 100, Schedulers.from(new WithinThreadExecutor()), record -> {
                  CompletableFuture<?> updated = indexUpdater.updateIndex(record.key, record.value, record.segment);
                  progressState.addItem(record.key, record.value, updated);
                  return updated;
               }));
            }
         }
         postIndex(indexUpdater, progressState, notifier);
      } else {
         DataConversion keyDataConversion = cache.getKeyDataConversion();
         Set<Class<?>> classSet = new HashSet<>(keys.size());
         AggregateCompletionStage<Void> updates = CompletionStages.aggregateCompletionStage();

         for (Object key : keys) {
            Object storedKey = keyDataConversion.toStorage(key);
            Object unwrappedKey = keyDataConversion.extractIndexable(storedKey, javaEmbeddedEntities);
            Object value = cache.get(key);
            if (value != null) {
               updates.dependsOn(indexUpdater.updateIndex(unwrappedKey, value, keyPartitioner.getSegment(storedKey)));
               classSet.add(value.getClass());
            }
         }

         if (!classSet.isEmpty()) {
            CompletableFutures.uncheckedAwait(updates.freeze().toCompletableFuture());
            indexUpdater.flush(classSet);
            indexUpdater.refresh(classSet);
         }
      }
      return null;
   }

   private void preIndex(AdvancedCache<Object, Object> cache, IndexUpdater indexUpdater, MassIndexerProgressNotifier notifier) {
      indexUpdater.purge(indexedTypes);
      notifier.notifyPreIndexingReloading();
      ComponentRegistryUtils.getSearchMapping(cache).reload();
      notifier.notifyIndexingStarting();
   }

   private void postIndex(IndexUpdater indexUpdater, MassIndexerProgressState progressState, MassIndexerProgressNotifier notifier) {
      progressState.waitForAsyncCompletion();
      indexUpdater.flush(indexedTypes);
      indexUpdater.refresh(indexedTypes);
      notifier.notifyIndexingCompletedSuccessfully();
   }

   record UpdateRecord(Object key, Object value, int segment) {
   }
}
