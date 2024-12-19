package org.infinispan.query.impl.massindex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.security.actions.SecurityActions;

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
   IndexWorker(String cacheName, MarshallableCollection<Class<?>> indexedTypes, boolean skipIndex, MarshallableCollection<Object> keys) {
      this(cacheName, MarshallableCollection.unwrap(indexedTypes), skipIndex, MarshallableCollection.unwrap(keys, HashSet::new));
   }

   @ProtoField(1)
   String getCacheName() {
      return cacheName;
   }

   @ProtoField(2)
   MarshallableCollection<Class<?>> getIndexedTypes() {
      return MarshallableCollection.create(indexedTypes);
   }

   @ProtoField(value = 3, defaultValue = "false")
   boolean isSkipIndex() {
      return skipIndex;
   }

   @ProtoField(4)
   MarshallableCollection<Object> getKeys() {
      return MarshallableCollection.create(keys);
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
               stream.forEach(entry -> {
                  Object key = entry.getKey();
                  Object value = valueDataConversion.extractIndexable(entry.getValue(), javaEmbeddedEntities);
                  int segment = keyPartitioner.getSegment(key);

                  if (value != null && indexUpdater.typeIsIndexed(value, indexedTypes)) {
                     progressState.addItem(key, value,
                           indexUpdater.updateIndex(key, value, segment));
                  }
               });
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
}
