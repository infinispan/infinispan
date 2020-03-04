package org.infinispan.query.impl.massindex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.time.TimeService;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * Mass indexer task.
 *
 * @author gustavonalle
 * @since 7.1
 */
public final class IndexWorker implements Function<EmbeddedCacheManager, Void> {

   private final String cacheName;
   private final Set<IndexedTypeIdentifier> indexedTypes;
   private final boolean flush;
   private final boolean clean;
   private final boolean skipIndex;
   private final boolean primaryOwner;
   private final Set<Object> keys;

   IndexWorker(String cacheName, Set<IndexedTypeIdentifier> indexedTypes, boolean flush, boolean clean, boolean skipIndex,
               boolean primaryOwner, Set<Object> keys) {
      this.cacheName = cacheName;
      this.indexedTypes = indexedTypes;
      this.flush = flush;
      this.clean = clean;
      this.skipIndex = skipIndex;
      this.primaryOwner = primaryOwner;
      this.keys = keys;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      AdvancedCache<Object, Object> cache = SecurityActions.getUnwrappedCache(embeddedCacheManager.getCache(cacheName)).getAdvancedCache();
      DataConversion valueDataConversion = cache.getValueDataConversion();
      Wrapper valueWrapper = valueDataConversion.getWrapper();
      boolean valueFilterable = valueWrapper.isFilterable();

      AdvancedCache<Object, Object> reindexCache = valueFilterable ? cache.withStorageMediaType() : cache;

      SearchIntegrator searchIntegrator = ComponentRegistryUtils.getSearchIntegrator(cache);
      KeyTransformationHandler keyTransformationHandler = ComponentRegistryUtils.getKeyTransformationHandler(cache);
      TimeService timeService = ComponentRegistryUtils.getTimeService(cache);

      IndexUpdater indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      ClusteringDependentLogic clusteringDependentLogic = SecurityActions.getClusteringDependentLogic(cache);
      KeyPartitioner keyPartitioner = ComponentRegistryUtils.getKeyPartitioner(cache);

      if (keys == null || keys.size() == 0) {
         preIndex(indexUpdater);
         if (!skipIndex) {
            KeyValueFilter filter = getFilter(clusteringDependentLogic, reindexCache.getKeyDataConversion());
            try (Stream<CacheEntry<Object, Object>> stream = reindexCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
                  .cacheEntrySet().stream()) {
               Iterator<CacheEntry<Object, Object>> iterator = stream.filter(CacheFilters.predicate(filter)).iterator();
               while (iterator.hasNext()) {
                  CacheEntry<Object, Object> next = iterator.next();
                  Object key = next.getKey();
                  Object storedKey = reindexCache.getKeyDataConversion().toStorage(key);
                  Object value = next.getValue();
                  if (valueFilterable) {
                     value = valueWrapper.wrap(value);
                  }
                  int segment = keyPartitioner.getSegment(storedKey);
                  if (value != null && indexedTypes.contains(PojoIndexedTypeIdentifier.convertFromLegacy(value.getClass()))) {
                     indexUpdater.updateIndex(next.getKey(), value, segment);
                  }
               }
            }
         }
         postIndex(indexUpdater);
      } else {
         Set<Class<?>> classSet = new HashSet<>();
         for (Object key : keys) {
            Object storedKey = reindexCache.getKeyDataConversion().toStorage(key);
            Object unwrappedKey = reindexCache.getKeyDataConversion().getWrapper().unwrap(storedKey);
            Object value = cache.get(key);
            if (value != null) {
               indexUpdater.updateIndex(unwrappedKey, value, keyPartitioner.getSegment(storedKey));
               classSet.add(value.getClass());
            }
         }
         Set<IndexedTypeIdentifier> toFlush = classSet.stream().map(PojoIndexedTypeIdentifier::convertFromLegacy).collect(Collectors.toSet());
         indexUpdater.flush(toFlush);
      }
      return null;
   }

   private void preIndex(IndexUpdater indexUpdater) {
      if (clean) indexUpdater.purge(indexedTypes);
   }

   private void postIndex(IndexUpdater indexUpdater) {
      indexUpdater.waitForAsyncCompletion();
      if (flush) indexUpdater.flush(indexedTypes);
   }

   private KeyValueFilter getFilter(ClusteringDependentLogic clusteringDependentLogic, DataConversion keyDataConversion) {
      return primaryOwner ? new PrimaryOwnersKeyValueFilter(clusteringDependentLogic, keyDataConversion) : AcceptAllKeyValueFilter.getInstance();
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
            for (IndexedTypeIdentifier indexedTypeIdentifier : worker.indexedTypes)
               output.writeObject(PojoIndexedTypeIdentifier.convertToLegacy(indexedTypeIdentifier));
         }
         output.writeBoolean(worker.flush);
         output.writeBoolean(worker.clean);
         output.writeBoolean(worker.primaryOwner);
         output.writeBoolean(worker.skipIndex);
         output.writeObject(worker.keys);
      }

      @Override
      public IndexWorker readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         int typesSize = input.readInt();
         Set<IndexedTypeIdentifier> types = new HashSet<>(typesSize);
         for (int i = 0; i < typesSize; i++) {
            types.add(PojoIndexedTypeIdentifier.convertFromLegacy((Class<?>) input.readObject()));
         }
         boolean flush = input.readBoolean();
         boolean clean = input.readBoolean();
         boolean primaryOwner = input.readBoolean();
         boolean skipIndex = input.readBoolean();
         Set<Object> keys = (Set<Object>) input.readObject();
         return new IndexWorker(cacheName, types, flush, clean, skipIndex, primaryOwner, keys);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_WORKER;
      }
   }

   private static class PrimaryOwnersKeyValueFilter implements KeyValueFilter<Object, Object> {
      private final ClusteringDependentLogic clusteringDependentLogic;
      private final DataConversion keyDataConversion;

      private PrimaryOwnersKeyValueFilter(ClusteringDependentLogic clusteringDependentLogic, DataConversion keyDataConversion) {
         this.clusteringDependentLogic = clusteringDependentLogic;
         this.keyDataConversion = keyDataConversion;
      }

      @Override
      public boolean accept(Object key, Object value, Metadata metadata) {
         return clusteringDependentLogic.getCacheTopology().getDistribution(keyDataConversion.toStorage(key)).isPrimary();
      }
   }
}
