package org.infinispan.query.impl.massindex;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingUtils;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * Base class for mass indexer tasks.
 *
 * @author gustavonalle
 * @since 7.1
 */
public class IndexWorker implements DistributedCallable<Object, Object, Void> {

   protected final IndexedTypeIdentifier indexedType;
   private final boolean flush;
   private final boolean clean;
   private final boolean primaryOwner;
   protected Cache<Object, Object> cache;
   protected IndexUpdater indexUpdater;
   private Set<Object> everywhereKeys;
   private Set<Object> keys = new HashSet<>();
   private ClusteringDependentLogic clusteringDependentLogic;
   private Encoder valueEncoder;
   private Wrapper valueWrapper;

   public IndexWorker(IndexedTypeIdentifier indexedType, boolean flush, boolean clean, boolean primaryOwner, Set<Object> everywhereKeys) {
      this.indexedType = indexedType;
      this.flush = flush;
      this.clean = clean;
      this.primaryOwner = primaryOwner;
      this.everywhereKeys = everywhereKeys;
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      this.cache = cache;
      this.indexUpdater = new IndexUpdater(cache);
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      this.clusteringDependentLogic = componentRegistry.getComponent(ClusteringDependentLogic.class);
      if (everywhereKeys != null && everywhereKeys.size() > 0)
         keys.addAll(everywhereKeys);
      if (inputKeys != null && inputKeys.size() > 0)
         keys.addAll(inputKeys);

      valueEncoder = cache.getAdvancedCache().getValueEncoder();
      valueWrapper = cache.getAdvancedCache().getValueWrapper();
   }

   protected void preIndex() {
      if (clean) indexUpdater.purge(indexedType);
   }

   protected void postIndex() {
      indexUpdater.waitForAsyncCompletion();
      if (flush) indexUpdater.flush(indexedType);
   }

   private KeyValueFilter getFilter() {
      return primaryOwner ? new PrimaryOwnersKeyValueFilter() : AcceptAllKeyValueFilter.getInstance();
   }

   private Object extractValue(Object storageValue) {
      return fromStorage(storageValue, valueEncoder, valueWrapper);
   }

   @Override
   @SuppressWarnings("unchecked")
   public Void call() throws Exception {
      Cache<Object, Object> unwrappedCache = SecurityActions.getUnwrappedCache(cache);
      if (keys == null || keys.size() == 0) {
         preIndex();
         KeyValueFilter filter = getFilter();
         try (Stream<CacheEntry<Object, Object>> stream = unwrappedCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
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
            Object value = extractValue(unwrappedCache.get(key));
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

   public static class Externalizer extends AbstractExternalizer<IndexWorker> {

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
         return new IndexWorker(PojoIndexedTypeIdentifier.convertFromLegacy((Class) input.readObject()), input.readBoolean(), input.readBoolean(), input.readBoolean(), (Set<Object>) input.readObject());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_WORKER;
      }
   }

   private class PrimaryOwnersKeyValueFilter implements KeyValueFilter {

      @Override
      public boolean accept(Object key, Object value, Metadata metadata) {
         return clusteringDependentLogic.getCacheTopology().getDistribution(key).isPrimary();
      }
   }

}
