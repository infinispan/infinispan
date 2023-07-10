package org.infinispan.query.impl.massindex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.externalizers.ExternalizerIds;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Mass indexer task.
 *
 * @author gustavonalle
 * @since 7.1
 */
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

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      AdvancedCache<Object, Object> cache = SecurityActions.getUnwrappedCache(embeddedCacheManager.getCache(cacheName)).getAdvancedCache();
      DataConversion valueDataConversion = cache.getValueDataConversion();

      AdvancedCache<Object, Object> reindexCache = cache.withStorageMediaType();

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      TimeService timeService = ComponentRegistryUtils.getTimeService(cache);

      MassIndexerProgressNotifier notifier = new MassIndexerProgressNotifier(searchMapping, timeService);
      IndexUpdater indexUpdater = new IndexUpdater(searchMapping);
      KeyPartitioner keyPartitioner = ComponentRegistryUtils.getKeyPartitioner(cache);

      if (keys == null || keys.size() == 0) {
         preIndex(cache, indexUpdater, notifier);
         MassIndexerProgressState progressState = new MassIndexerProgressState(notifier);
         if (!skipIndex) {
            try (Stream<CacheEntry<Object, Object>> stream = reindexCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
                  .cacheEntrySet().stream()) {
               stream.forEach(entry -> {
                  Object key = entry.getKey();
                  Object value = valueDataConversion.extractIndexable(entry.getValue());
                  int segment = keyPartitioner.getSegment(key);

                  if (value != null && indexedTypes.contains(indexUpdater.toConvertedEntityJavaClass(value))) {
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
            Object unwrappedKey = keyDataConversion.extractIndexable(storedKey);
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

   public static final class Externalizer extends AbstractExternalizer<IndexWorker> {

      @Override
      public Set<Class<? extends IndexWorker>> getTypeClasses() {
         return Collections.singleton(IndexWorker.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IndexWorker worker) throws IOException {
         output.writeObject(worker.cacheName);
         if (worker.indexedTypes == null) {
            output.writeInt(0);
         } else {
            output.writeInt(worker.indexedTypes.size());
            for (Class<?> entityType : worker.indexedTypes)
               output.writeObject(entityType);
         }
         output.writeBoolean(worker.skipIndex);
         output.writeObject(worker.keys);
      }

      @Override
      public IndexWorker readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         int typesSize = input.readInt();
         Set<Class<?>> types = new HashSet<>(typesSize);
         for (int i = 0; i < typesSize; i++) {
            types.add((Class<?>) input.readObject());
         }
         boolean skipIndex = input.readBoolean();
         Set<Object> keys = (Set<Object>) input.readObject();
         return new IndexWorker(cacheName, types, skipIndex, keys);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_WORKER;
      }
   }

}
