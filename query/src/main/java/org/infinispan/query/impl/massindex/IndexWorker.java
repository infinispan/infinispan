package org.infinispan.query.impl.massindex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
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
   private final IndexedTypeIdentifier indexedType;
   private final boolean flush;
   private final boolean clean;
   private final boolean primaryOwner;
   private final Set<Object> keys;

   IndexWorker(String cacheName, IndexedTypeIdentifier indexedType, boolean flush, boolean clean, boolean primaryOwner,
         Set<Object> keys) {
      this.cacheName = cacheName;
      this.indexedType = indexedType;
      this.flush = flush;
      this.clean = clean;
      this.primaryOwner = primaryOwner;
      this.keys = keys;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = embeddedCacheManager.getCache(cacheName);
      AdvancedCache<Object, Object> unwrapped = SecurityActions.getUnwrappedCache(cache).getAdvancedCache();
      if (unwrapped.getCacheConfiguration().memory().storageType() == StorageType.OBJECT) {
         cache = unwrapped.withWrapping(ByteArrayWrapper.class, IdentityWrapper.class);
      } else {
         cache = cache;  //todo [anistor] why not `unwrapped` instead ? do we need security for mass indexing ?
      }

      SearchIntegrator searchIntegrator = ComponentRegistryUtils.getSearchIntegrator(unwrapped);
      KeyTransformationHandler keyTransformationHandler = ComponentRegistryUtils.getKeyTransformationHandler(unwrapped);
      TimeService timeService = ComponentRegistryUtils.getTimeService(unwrapped);

      IndexUpdater indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      ClusteringDependentLogic clusteringDependentLogic = SecurityActions.getClusteringDependentLogic(unwrapped);

      DataConversion keyDataConversion = unwrapped.getKeyDataConversion();
      DataConversion valueDataConversion = unwrapped.getValueDataConversion();

      if (keys == null || keys.size() == 0) {
         preIndex(indexUpdater);
         KeyValueFilter filter = getFilter(clusteringDependentLogic, keyDataConversion);
         try (Stream<CacheEntry<Object, Object>> stream = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
               .cacheEntrySet().stream()) {
            Iterator<CacheEntry<Object, Object>> iterator = stream.filter(CacheFilters.predicate(filter)).iterator();
            while (iterator.hasNext()) {
               CacheEntry<Object, Object> next = iterator.next();
               Object value = extractValue(next.getValue(), valueDataConversion);
               //TODO do not use Class equality but refactor to type equality:
               if (value != null && value.getClass().equals(indexedType.getPojoType()))
                  indexUpdater.updateIndex(next.getKey(), value);
            }
         }
         postIndex(indexUpdater);
      } else {
         Set<Class<?>> classSet = new HashSet<>();
         for (Object key : keys) {
            Object value = extractValue(cache.get(key), valueDataConversion);
            if (value != null) {
               indexUpdater.updateIndex(key, value);
               classSet.add(value.getClass());
            }
         }
         for (Class<?> clazz : classSet)
            indexUpdater.flush(PojoIndexedTypeIdentifier.convertFromLegacy(clazz));
      }
      return null;
   }

   private void preIndex(IndexUpdater indexUpdater) {
      if (clean) indexUpdater.purge(indexedType);
   }

   private void postIndex(IndexUpdater indexUpdater) {
      indexUpdater.waitForAsyncCompletion();
      if (flush) indexUpdater.flush(indexedType);
   }

   private KeyValueFilter getFilter(ClusteringDependentLogic clusteringDependentLogic, DataConversion keyDataConversion) {
      return primaryOwner ? new PrimaryOwnersKeyValueFilter(clusteringDependentLogic, keyDataConversion) : AcceptAllKeyValueFilter.getInstance();
   }

   private Object extractValue(Object storageValue, DataConversion valueDataConversion) {
      return valueDataConversion.extractIndexable(storageValue);
   }

   public static final class Externalizer extends AbstractExternalizer<IndexWorker> {

      @Override
      public Set<Class<? extends IndexWorker>> getTypeClasses() {
         return Collections.singleton(IndexWorker.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IndexWorker worker) throws IOException {
         output.writeObject(worker.cacheName);
         output.writeObject(PojoIndexedTypeIdentifier.convertToLegacy(worker.indexedType));
         output.writeBoolean(worker.flush);
         output.writeBoolean(worker.clean);
         output.writeBoolean(worker.primaryOwner);
         output.writeObject(worker.keys);
      }

      @Override
      public IndexWorker readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         Class indexedClass = (Class) input.readObject();
         boolean flush = input.readBoolean();
         boolean clean = input.readBoolean();
         boolean primaryOwner = input.readBoolean();
         Set<Object> keys = (Set<Object>) input.readObject();
         return new IndexWorker(cacheName, PojoIndexedTypeIdentifier.convertFromLegacy(indexedClass), flush, clean,
               primaryOwner, keys);
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
