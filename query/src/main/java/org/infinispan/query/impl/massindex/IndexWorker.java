package org.infinispan.query.impl.massindex;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ProgressTracker;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableSet;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.reactive.publisher.PublisherTransformers;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.security.actions.SecurityActions;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Mass indexer task.
 *
 * @author gustavonalle
 * @since 7.1
 */
@ProtoTypeId(ProtoStreamTypeIds.INDEX_WORKER)
public final class IndexWorker implements Function<EmbeddedCacheManager, Void> {

   // Maximum number of index updates in flight while reindexing.
   private static final int MAX_CONCURRENT_UPDATES = 100;

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
      DataConversion keyDataConversion = cache.getKeyDataConversion();
      DataConversion valueDataConversion = cache.getValueDataConversion();

      AdvancedCache<Object, Object> reindexCache = cache.withStorageMediaType();
      Configuration cfg = SecurityActions.getCacheConfiguration(reindexCache);
      boolean javaEmbeddedEntities = cfg.indexing().useJavaEmbeddedEntities();

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      TimeService timeService = ComponentRegistryUtils.getTimeService(cache);
      ScheduledExecutorService timeoutExecutor = ComponentRegistryUtils.getTimeoutScheduledExecutor(cache);

      MassIndexerProgressNotifier notifier = new MassIndexerProgressNotifier(searchMapping, timeService);
      ProgressTracker progressTracker = new ProgressTracker("query-indexer-" + cacheName, timeoutExecutor, timeService, cfg.clustering().remoteTimeout(), TimeUnit.MILLISECONDS);
      IndexUpdater indexUpdater = new IndexUpdater(searchMapping);
      KeyPartitioner keyPartitioner = ComponentRegistryUtils.getKeyPartitioner(cache);

      try {
         if (keys == null || keys.isEmpty()) {
            preIndex(cache, indexUpdater, notifier);
            MassIndexerProgressState progressState = new MassIndexerProgressState(notifier, progressTracker);

            if (!skipIndex) {
               progressTracker.addTasks(reindexCache.withFlags(Flag.CACHE_MODE_LOCAL).size());

               // Iterate this node's entries (memory and stores) without blocking.
               // A blocking iterator here would pull the next entry using the thread that completed the previous pull from the iteration.
               // This means the blocking iteration could land on a non-blocking thread.
               ClusterPublisherManager<Object, Object> publisherManager = ComponentRegistryUtils.getLocalClusterPublisherManager(cache);
               Publisher<CacheEntry<Object, Object>> entries = publisherManager.entryPublisher(null, null, null,
                     FlagBitSets.CACHE_MODE_LOCAL, DeliveryGuarantee.AT_MOST_ONCE, cfg.clustering().stateTransfer().chunkSize(), PublisherTransformers.identity()
               ).publisherWithoutSegments();

               CompletionStage<Void> indexed = Flowable.fromPublisher(entries)
                     .mapOptional(entry -> {
                        // The publisher emits the entries in the storage format.
                        // We must convert them back to a format the indexer can work with.
                        Object storedKey = entry.getKey();
                        Object value = valueDataConversion.extractIndexable(entry.getValue(), javaEmbeddedEntities);
                        if (value != null && indexUpdater.typeIsIndexed(value, indexedTypes)) {
                           Object key = keyDataConversion.extractIndexable(storedKey, javaEmbeddedEntities);
                           return Optional.of(new UpdateRecord(key, value, keyPartitioner.getSegment(storedKey)));
                        }
                        return Optional.empty();
                     })
                     .flatMapCompletable(record -> {
                        CompletableFuture<?> updated = indexUpdater.updateIndex(record.key, record.value, record.segment);
                        progressState.addItem(record.key, record.value, updated);
                        return Completable.fromCompletionStage(updated);
                     }, false, MAX_CONCURRENT_UPDATES)
                     .toCompletionStage(null);
               join(indexed);
            }
            postIndex(indexUpdater, progressState, notifier);
         } else {
            Set<Class<?>> classSet = new HashSet<>(keys.size());
            AggregateCompletionStage<Void> updates = CompletionStages.aggregateCompletionStage();

            progressTracker.addTasks(keys.size());

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
      } catch (Throwable t) {
         progressTracker.failed(t);
         throw t;
      }
      progressTracker.finishedAllTasks();
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
