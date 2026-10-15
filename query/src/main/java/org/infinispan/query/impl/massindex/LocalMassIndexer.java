package org.infinispan.query.impl.massindex;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ProgressTracker;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
 * Rebuilds the index of a cache from the entries held by this member.
 *
 * <p>
 * At most one rebuild runs at a time on a member, whether it was started for this member only or as part of a
 * cluster-wide rebuild. Rebuilds on different members don't affect each other.
 * </p>
 *
 * @since 16.4
 */
@Scope(Scopes.NAMED_CACHE)
public final class LocalMassIndexer {

   // Maximum number of index updates in flight while reindexing.
   private static final int MAX_CONCURRENT_UPDATES = 100;

   private final AdvancedCache<Object, Object> cache;

   // Acts as a permit pass for reindexing.
   // The permit is held for the whole rebuild of this member's index.
   private final AtomicBoolean running = new AtomicBoolean();

   public LocalMassIndexer(AdvancedCache<Object, Object> cache) {
      this.cache = SecurityActions.getUnwrappedCache(cache).getAdvancedCache();
   }

   /**
    * Rebuilds this member's index of the given types from the entries this member holds, or only removes it, and
    * blocks until done.
    *
    * <p>
    * <b>Warn:</b> Must be called from a blocking thread.
    * </p>
    *
    * @param indexedTypes the types whose index to rebuild.
    * @param skipIndex    {@code true} to only remove the index of the types, without indexing the entries again.
    * @throws MassIndexerAlreadyStartedException if a rebuild is already running on this member.
    */
   public void reindex(Collection<Class<?>> indexedTypes, boolean skipIndex) {
      if (!running.compareAndSet(false, true)) {
         throw new MassIndexerAlreadyStartedException();
      }

      try {
         doReindex(indexedTypes, skipIndex);
      } finally {
         running.set(false);
      }
   }

   /**
    * Returns whether a rebuild of this member's index is in progress.
    *
    * @return {@code true} while a rebuild of this member's index is running, {@code false} otherwise.
    */
   public boolean isRunning() {
      return running.get();
   }

   private void doReindex(Collection<Class<?>> indexedTypes, boolean skipIndex) {
      DataConversion keyDataConversion = cache.getKeyDataConversion();
      DataConversion valueDataConversion = cache.getValueDataConversion();
      AdvancedCache<Object, Object> reindexCache = cache.withStorageMediaType();
      Configuration cfg = SecurityActions.getCacheConfiguration(reindexCache);
      boolean javaEmbeddedEntities = cfg.indexing().useJavaEmbeddedEntities();

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      TimeService timeService = ComponentRegistryUtils.getTimeService(cache);
      ScheduledExecutorService timeoutExecutor = ComponentRegistryUtils.getTimeoutScheduledExecutor(cache);

      MassIndexerProgressNotifier notifier = new MassIndexerProgressNotifier(searchMapping, timeService);
      ProgressTracker progressTracker = new ProgressTracker("query-indexer-" + cache.getName(), timeoutExecutor, timeService, cfg.clustering().remoteTimeout(), TimeUnit.MILLISECONDS);
      IndexUpdater indexUpdater = new IndexUpdater(searchMapping);
      KeyPartitioner keyPartitioner = ComponentRegistryUtils.getKeyPartitioner(cache);

      try {
         preIndex(cache, indexedTypes, indexUpdater, notifier);
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
         postIndex(indexUpdater, indexedTypes, progressState, notifier);
      } catch (Throwable t) {
         progressTracker.failed(t);
         throw t;
      }
      progressTracker.finishedAllTasks();
   }

   private void preIndex(AdvancedCache<Object, Object> cache, Collection<Class<?>> indexedTypes, IndexUpdater indexUpdater, MassIndexerProgressNotifier notifier) {
      indexUpdater.purge(indexedTypes);
      notifier.notifyPreIndexingReloading();
      ComponentRegistryUtils.getSearchMapping(cache).reload();
      notifier.notifyIndexingStarting();
   }

   private void postIndex(IndexUpdater indexUpdater, Collection<Class<?>> indexedTypes, MassIndexerProgressState progressState, MassIndexerProgressNotifier notifier) {
      progressState.waitForAsyncCompletion();
      indexUpdater.flush(indexedTypes);
      indexUpdater.refresh(indexedTypes);
      notifier.notifyIndexingCompletedSuccessfully();
   }

   private record UpdateRecord(Object key, Object value, int segment) { }
}
