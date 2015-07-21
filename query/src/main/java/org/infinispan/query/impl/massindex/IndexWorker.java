package org.infinispan.query.impl.massindex;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Base class for mass indexer tasks.
 *
 * @author gustavonalle
 * @since 7.1
 */
public class IndexWorker implements DistributedCallable<Object, Object, Object> {

   protected Cache<Object, Object> cache;
   protected final Class<?> entity;
   private final boolean flush;
   protected IndexUpdater indexUpdater;

   private ClusteringDependentLogic clusteringDependentLogic;

   public IndexWorker(Class<?> entity, boolean flush) {
      this.entity = entity;
      this.flush = flush;
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      this.cache = cache;
      this.indexUpdater = new IndexUpdater(cache);
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      this.clusteringDependentLogic = componentRegistry.getComponent(ClusteringDependentLogic.class);
   }

   protected void preIndex() {
      if (flush) indexUpdater.purge(entity);
   }

   protected void postIndex() {
      if (flush) indexUpdater.flush(entity);
   }

   private KeyValueFilter getFilter() {
      boolean replicated = cache.getCacheConfiguration().clustering().cacheMode().isReplicated();
      return replicated ? AcceptAllKeyValueFilter.getInstance() : new PrimaryOwnersKeyValueFilter();
   }

   private Object extractValue(Object wrappedValue) {
      if (wrappedValue instanceof MarshalledValue)
         return ((MarshalledValue) wrappedValue).get();
      return wrappedValue;
   }

   @Override
   @SuppressWarnings("unchecked")
   public Void call() throws Exception {
      preIndex();
      KeyValueFilter filter = getFilter();
      try (Stream<CacheEntry<Object, Object>> stream = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL)
              .cacheEntrySet().stream()) {
         Iterator<CacheEntry<Object, Object>> iterator = stream.filter(CacheFilters.predicate(filter)).iterator();
         while (iterator.hasNext()) {
            CacheEntry<Object, Object> next = iterator.next();
            Object value = extractValue(next.getValue());
            if (value != null && value.getClass().equals(entity))
               indexUpdater.updateIndex(next.getKey(), value);
         }
      }
      postIndex();
      return null;
   }


   private class PrimaryOwnersKeyValueFilter implements KeyValueFilter {

      @Override
      public boolean accept(Object key, Object value, Metadata metadata) {
         return clusteringDependentLogic.localNodeIsPrimaryOwner(key);
      }
   }

   public static class Externalizer extends AbstractExternalizer<IndexWorker> {

      @Override
      @SuppressWarnings("ALL")
      public Set<Class<? extends IndexWorker>> getTypeClasses() {
         return Util.<Class<? extends IndexWorker>>asSet(IndexWorker.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IndexWorker worker) throws IOException {
         output.writeObject(worker.entity);
         output.writeBoolean(worker.flush);
      }

      @Override
      public IndexWorker readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new IndexWorker((Class<?>) input.readObject(), input.readBoolean());
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INDEX_WORKER;
      }
   }

}
