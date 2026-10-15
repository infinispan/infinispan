package org.infinispan.query.impl.massindex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableSet;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.mapper.mapping.SearchMapping;
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
      // Without keys, the worker rebuilds the whole index of this member, guarded by the member's local mass indexer.
      if (keys == null || keys.isEmpty()) {
         LocalMassIndexer lmi = ComponentRegistryUtils.getLocalMassIndexer(cache);
         lmi.reindex(indexedTypes, skipIndex);
         return null;
      }

      AdvancedCache<Object, Object> reindexCache = cache.withStorageMediaType();
      Configuration cfg = SecurityActions.getCacheConfiguration(reindexCache);
      boolean javaEmbeddedEntities = cfg.indexing().useJavaEmbeddedEntities();

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      TimeService timeService = ComponentRegistryUtils.getTimeService(cache);
      ScheduledExecutorService timeoutExecutor = ComponentRegistryUtils.getTimeoutScheduledExecutor(cache);

      ProgressTracker progressTracker = new ProgressTracker("query-indexer-" + cacheName, timeoutExecutor, timeService, cfg.clustering().remoteTimeout(), TimeUnit.MILLISECONDS);
      IndexUpdater indexUpdater = new IndexUpdater(searchMapping);
      KeyPartitioner keyPartitioner = ComponentRegistryUtils.getKeyPartitioner(cache);

      DataConversion keyDataConversion = cache.getKeyDataConversion();
      Set<Class<?>> classSet = new HashSet<>(keys.size());
      AggregateCompletionStage<Void> updates = CompletionStages.aggregateCompletionStage();

      try {
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
      } catch (Throwable t) {
         progressTracker.failed(t);
         throw t;
      }
      progressTracker.finishedAllTasks();
      return null;
   }
}
