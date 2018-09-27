package org.infinispan.query.impl.massindex;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.encoding.DataConversion;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
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
public final class IndexWorker implements DistributedCallable<Object, Object, Void> {

   private final IndexedTypeIdentifier indexedType;
   private final boolean flush;
   private final boolean clean;
   private final boolean primaryOwner;
   private final Set<Object> everywhereKeys;
   private Cache<Object, Object> cache;
   private IndexUpdater indexUpdater;
   private Set<Object> keys = new HashSet<>();
   private ClusteringDependentLogic clusteringDependentLogic;
   private DataConversion valueDataConversion;
   private DataConversion keyDataConversion;

   IndexWorker(IndexedTypeIdentifier indexedType, boolean flush, boolean clean, boolean primaryOwner, Set<Object> everywhereKeys) {
      this.indexedType = indexedType;
      this.flush = flush;
      this.clean = clean;
      this.primaryOwner = primaryOwner;
      this.everywhereKeys = everywhereKeys;
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      AdvancedCache<Object, Object> unwrapped = SecurityActions.getUnwrappedCache(cache).getAdvancedCache();
      if (unwrapped.getCacheConfiguration().memory().storageType() == StorageType.OBJECT) {
         this.cache = unwrapped.withWrapping(ByteArrayWrapper.class, IdentityWrapper.class);
      } else {
         this.cache = cache;  //todo [anistor] why not `unwrapped` instead ? do we need security for mass indexing ?
      }

      SearchIntegrator searchIntegrator = ComponentRegistryUtils.getSearchIntegrator(unwrapped);
      KeyTransformationHandler keyTransformationHandler = ComponentRegistryUtils.getKeyTransformationHandler(unwrapped);
      TimeService timeService = ComponentRegistryUtils.getTimeService(unwrapped);

      this.indexUpdater = new IndexUpdater(searchIntegrator, keyTransformationHandler, timeService);
      this.clusteringDependentLogic = SecurityActions.getClusteringDependentLogic(unwrapped);

      if (everywhereKeys != null)
         keys.addAll(everywhereKeys);
      if (inputKeys != null)
         keys.addAll(inputKeys);

      keyDataConversion = unwrapped.getKeyDataConversion();
      valueDataConversion = unwrapped.getValueDataConversion();
   }

   private void preIndex() {
      if (clean) indexUpdater.purge(indexedType);
   }

   private void postIndex() {
      indexUpdater.waitForAsyncCompletion();
      if (flush) indexUpdater.flush(indexedType);
   }

   private KeyValueFilter getFilter() {
      return primaryOwner ? new PrimaryOwnersKeyValueFilter() : AcceptAllKeyValueFilter.getInstance();
   }

   private Object extractValue(Object storageValue) {
      return valueDataConversion.extractIndexable(storageValue);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Void call() {
      if (keys == null || keys.size() == 0) {
         preIndex();
         KeyValueFilter filter = getFilter();
         try (Stream<CacheEntry<Object, Object>> stream = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
               .cacheEntrySet().stream()) {
            Iterator<CacheEntry<Object, Object>> iterator = stream.filter(CacheFilters.predicate(filter)).iterator();
            while (iterator.hasNext()) {
               CacheEntry<Object, Object> next = iterator.next();
               Object value = extractValue(next.getValue());
               //TODO do not use Class equality but refactor to type equality:
               if (value != null && value.getClass().equals(indexedType.getPojoType()))
                  indexUpdater.updateIndex(next.getKey(), value);
            }
         }
         postIndex();
      } else {
         Set<Class<?>> classSet = new HashSet<>();
         for (Object key : keys) {
            Object value = extractValue(cache.get(key));
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

   public static final class Externalizer extends AbstractExternalizer<IndexWorker> {

      @Override
      public Set<Class<? extends IndexWorker>> getTypeClasses() {
         return Collections.singleton(IndexWorker.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IndexWorker worker) throws IOException {
         output.writeObject(PojoIndexedTypeIdentifier.convertToLegacy(worker.indexedType));
         output.writeBoolean(worker.flush);
         output.writeBoolean(worker.clean);
         output.writeBoolean(worker.primaryOwner);
         output.writeObject(worker.everywhereKeys);
      }

      @Override
      public IndexWorker readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Class indexedClass = (Class) input.readObject();
         boolean flush = input.readBoolean();
         boolean clean = input.readBoolean();
         boolean primaryOwner = input.readBoolean();
         Set<Object> everywhereKeys = (Set<Object>) input.readObject();
         return new IndexWorker(PojoIndexedTypeIdentifier.convertFromLegacy(indexedClass), flush, clean, primaryOwner, everywhereKeys);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_WORKER;
      }
   }

   private class PrimaryOwnersKeyValueFilter implements KeyValueFilter<Object, Object> {

      @Override
      public boolean accept(Object key, Object value, Metadata metadata) {
         return clusteringDependentLogic.getCacheTopology().getDistribution(keyDataConversion.toStorage(key)).isPrimary();
      }
   }
}
