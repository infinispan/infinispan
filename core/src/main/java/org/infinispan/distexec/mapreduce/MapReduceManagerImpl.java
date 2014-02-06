package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.CollectionKeyFilter;
import org.infinispan.persistence.CompositeFilter;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.PrimaryOwnerFilter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.infinispan.distexec.mapreduce.MapReduceTask.DEFAULT_TMP_CACHE_CONFIGURATION_NAME;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * Default implementation of {@link MapReduceManager}.
 * <p>
 *
 *
 * This is an internal class, not intended to be used by clients.
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class MapReduceManagerImpl implements MapReduceManager {

   private static final Log log = LogFactory.getLog(MapReduceManagerImpl.class);
   private ClusteringDependentLogic cdl;
   private EmbeddedCacheManager cacheManager;
   private PersistenceManager persistenceManager;
   private ExecutorService executorService;
   private TimeService timeService;
   private int chunkSize;

   MapReduceManagerImpl() {
   }

   @Inject
   public void init(EmbeddedCacheManager cacheManager, PersistenceManager persistenceManager,
            @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
            ClusteringDependentLogic cdl, TimeService timeService, Configuration configuration) {
      this.cacheManager = cacheManager;
      this.persistenceManager = persistenceManager;
      this.cdl = cdl;
      this.executorService = asyncTransportExecutor;
      this.timeService = timeService;
      this.chunkSize = configuration.clustering().stateTransfer().chunkSize();
   }

   @Override
   public ExecutorService getExecutorService() {
      return executorService;
   }

   @Override
   public <KIn, VIn, KOut, VOut> Map<KOut, List<VOut>> mapAndCombineForLocalReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      CollectableCollector<KOut, VOut> collector = map(mcc);
      return combineForLocalReduction(mcc, collector);
   }

   @Override
   public <KIn, VIn, KOut, VOut> Set<KOut> mapAndCombineForDistributedReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      CollectableCollector<KOut, VOut> collector = map(mcc);
      try {
         return combine(mcc, collector);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   @SuppressWarnings("unchecked")
   public <KOut, VOut> Map<KOut, VOut> reduce(ReduceCommand<KOut, VOut> reduceCommand) throws InterruptedException {
      final Set<KOut> keys = reduceCommand.getKeys();
      final String taskId = reduceCommand.getTaskId();
      boolean noInputKeys = keys == null || keys.isEmpty();
      final Map<KOut, VOut> result = CollectionFactory.makeConcurrentMap(256);
      if (noInputKeys) {
         //illegal state, raise exception
         throw new IllegalStateException("Reduce phase of MapReduceTask " + taskId + " on node " + cdl.getAddress()
               + " executed with empty input keys");
      } else {
         final Reducer<KOut, VOut> reducer = reduceCommand.getReducer();
         final boolean useIntermediateKeys = reduceCommand.isEmitCompositeIntermediateKeys();         
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         log.tracef("For m/r task %s invoking %s at %s", taskId, reduceCommand, cdl.getAddress());
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            //first hook into lifecycle
            Cache<?, ?> cache = cacheManager.getCache(reduceCommand.getCacheName());
            taskLifecycleService.onPreExecute(reducer, cache);
            AdvancedCacheLoader.KeyFilter<?> filter = null;
            if (useIntermediateKeys) {
               //shared tmp cache, filter keys that belong to this task
               filter = new IntermediateKeyFilter<KOut>(taskId);
            } else {
               //dedicated tmp cache, all keys belong to this task
               filter = AdvancedCacheLoader.KeyFilter.LOAD_ALL_FILTER;
            }
            //iterate all tmp cache entries in memory, do it in parallel
            DataContainer dc = cache.getAdvancedCache().getDataContainer();
            dc.executeTask(filter, new StatelessDataContainerTask<KOut, List<VOut>>() {
               @Override
               public void apply(Object k, InternalCacheEntry v) {
                  KOut key = null;
                  if (useIntermediateKeys) {
                     IntermediateCompositeKey<KOut> intKey = (IntermediateCompositeKey<KOut>) k;
                     key = intKey.getKey();
                  } else {
                     key = (KOut) k;
                  }
                  //resolve List<VOut> for iterated key stored in tmp cache
                  List<VOut> value = getValue(v);
                  if (value != null) {
                     // and reduce it
                     VOut reduced = reducer.reduce(key, value.iterator());
                     result.put(key, reduced);
                     log.tracef("For m/r task %s reduced %s to %s at %s ", taskId, key, reduced, cdl.getAddress());
                  }
               }
            });
         } finally {
            if (log.isTraceEnabled()) {
               log.tracef("Reduce for task %s took %s milliseconds", reduceCommand.getTaskId(),
                     timeService.timeDuration(start, TimeUnit.MILLISECONDS));
            }
            taskLifecycleService.onPostExecute(reducer);
         }
      }
      return result;
   }

   @SuppressWarnings("unchecked")
   protected <KIn, VIn, KOut, VOut> CollectableCollector<KOut, VOut> map(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      final Cache<KIn, VIn> cache = cacheManager.getCache(mcc.getCacheName());
      Set<KIn> keys = mcc.getKeys();
      final Mapper<KIn, VIn, KOut, VOut> mapper = mcc.getMapper();
      final boolean inputKeysSpecified = keys != null && !keys.isEmpty();

      // hook map function into lifecycle and execute it
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
      final DefaultCollector<KOut, VOut> collector = new DefaultCollector<KOut, VOut>(!inputKeysSpecified);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      log.tracef("For m/r task %s invoking %s with input keys %s",  mcc.getTaskId(), mcc, keys);
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      try {
         taskLifecycleService.onPreExecute(mapper, cache);
         //User specified input taks keys, most likely a short list of input keys (<10^3), iterate serially
         if (inputKeysSpecified) {
            for (KIn key : keys) {
               VIn value = cache.get(key);
               mapper.map(key, value, collector);
            }
         } else {
            // here we have to iterate all entries in memory, do it in parallel
            dc.executeTask(new PrimaryOwnerFilter(cdl), new StatelessDataContainerTask<KIn,VIn>() {
               @Override
               public void apply(Object key , InternalCacheEntry v) {
                  VIn value = getValue(v);
                  if (value != null) {
                     mapper.map((KIn)key, value, collector);
                  }
               }
            });
         }
         // in case we have stores, we have to process key/values from there as well
         if (persistenceManager != null && !inputKeysSpecified) {
               AdvancedCacheLoader.KeyFilter<?> keyFilter = new CompositeFilter(new PrimaryOwnerFilter(cdl), new CollectionKeyFilter(dc.keySet()));
               persistenceManager.processOnAllStores(keyFilter, new MapReduceCacheLoaderTask(mapper, collector),
                     true, false);
         }
      } finally {
         if (log.isTraceEnabled()) {
            log.tracef("Map phase for task %s took %s milliseconds",
                       mcc.getTaskId(), timeService.timeDuration(start, TimeUnit.MILLISECONDS));
         }
         taskLifecycleService.onPostExecute(mapper);
      }
      return collector;
   }

   protected <KIn, VIn, KOut, VOut> Set<KOut> combine(MapCombineCommand<KIn, VIn, KOut, VOut> mcc,
            CollectableCollector<KOut, VOut> collector) throws Exception{

      String taskId =  mcc.getTaskId();
      Set<KOut> mapPhaseKeys = new HashSet<KOut>();
      Cache<Object, DeltaAwareList<VOut>> tmpCache = null;
      if (mcc.isEmitCompositeIntermediateKeys()) {
         tmpCache = cacheManager.getCache(DEFAULT_TMP_CACHE_CONFIGURATION_NAME);
      } else {
         tmpCache = cacheManager.getCache(taskId);
      }
      if (tmpCache == null) {
         throw new IllegalStateException("Temporary cache for MapReduceTask " + taskId
                  + " not found on " + cdl.getAddress());
      }
      if (mcc.hasCombiner()) {
         Reducer <KOut,VOut> combiner = mcc.getCombiner();
         Cache<?, ?> cache = cacheManager.getCache(mcc.getCacheName());
         log.tracef("For m/r task %s invoking combiner %s at %s",  taskId, mcc, cdl.getAddress());
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         Map<KOut, List<VOut>> combinedMap = new ConcurrentHashMap<KOut, List<VOut>>();
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            taskLifecycleService.onPreExecute(combiner, cache);
            Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
            for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
               List<VOut> mapped = e.getValue();
               List<VOut> combined;
               if (mapped.size() == 1) {
                  combined = mapped;
               } else {
                  combined = Arrays.asList(combiner.reduce(e.getKey(), mapped.iterator()));
               }
               combinedMap.put(e.getKey(), combined);
               log.tracef("For m/r task %s combined %s to %s at %s" , taskId, e.getKey(), combined, cdl.getAddress());
            }
         } finally {
            if (log.isTraceEnabled()) {
               log.tracef("Combine for task %s took %s milliseconds", mcc.getTaskId(),
                          timeService.timeDuration(start, TimeUnit.MILLISECONDS));
            }
            taskLifecycleService.onPostExecute(combiner);
         }
         migrateIntermediateKeys(mcc, mapPhaseKeys, tmpCache, combinedMap);
      } else {
         // Combiner not specified so lets insert each key/uncombined-List pair into tmp cache
         Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
         migrateIntermediateKeys(mcc, mapPhaseKeys, tmpCache, collectedValues);
      }
      return mapPhaseKeys;
   }

   private <KIn, VIn, KOut, VOut> void migrateIntermediateKeys(MapCombineCommand<KIn, VIn, KOut, VOut> mcc,
         Set<KOut> mapPhaseKeys, Cache<Object, DeltaAwareList<VOut>> tmpCache, Map<KOut, List<VOut>> collectedValues) {
      DistributionManager dm = tmpCache.getAdvancedCache().getDistributionManager();
      String taskId = mcc.getTaskId();
      boolean emitCompositeIntermediateKeys = mcc.isEmitCompositeIntermediateKeys();
      Map<Address, List<KOut>> keysToNodes = mapKeysToNodes(dm, taskId, collectedValues.keySet(),
            emitCompositeIntermediateKeys);
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      try {
         for (Entry<Address, List<KOut>> entry : keysToNodes.entrySet()) {
            List<KOut> keysHashedToAddress = entry.getValue();
            try {
               log.tracef("For m/r task %s migrating intermediate keys %s to %s", taskId, keysHashedToAddress, entry.getKey());
               for (KOut key : keysHashedToAddress) {
                  List<VOut> values = collectedValues.get(key);
                  int entryTransferCount = chunkSize > 0 ? chunkSize :values.size();
                  for (int i = 0; i < values.size(); i += entryTransferCount) {
                     List<VOut> chunk = values.subList(i, Math.min(values.size(), i + entryTransferCount));
                     DeltaAwareList<VOut> delta = new DeltaAwareList<VOut>(chunk);
                     if (emitCompositeIntermediateKeys) {
                        tmpCache.put(new IntermediateCompositeKey<KOut>(taskId, key), delta);
                     } else {
                        tmpCache.put(key, delta);
                     }
                  }
                  mapPhaseKeys.add(key);
               }
            } catch (Exception e) {
               throw new CacheException("Could not move intermediate keys/values for M/R task " + taskId, e);
            }
         }
      } finally {
         if (log.isTraceEnabled()) {
            log.tracef("Migrating keys for task %s took %s milliseconds (Migrated %s keys)",
                  mcc.getTaskId(), timeService.timeDuration(start, TimeUnit.MILLISECONDS), mapPhaseKeys.size());
         }
      }
   }

   private <KIn, VIn, KOut, VOut> Map<KOut, List<VOut>> combineForLocalReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc,
            CollectableCollector<KOut, VOut> collector) {

      String taskId =  mcc.getTaskId();
      Reducer <KOut,VOut> combiner = mcc.getCombiner();
      Map<KOut, List<VOut>> result;

      if (combiner != null) {
         result = new HashMap<KOut, List<VOut>>();
         log.tracef("For m/r task %s invoking combiner %s at %s",  taskId, mcc, cdl.getAddress());
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            Cache<?, ?> cache = cacheManager.getCache(mcc.getCacheName());
            taskLifecycleService.onPreExecute(combiner, cache);
            Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
            for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
               VOut combined;
               List<VOut> list = e.getValue();
               List<VOut> l = new LinkedList<VOut>();
               if (list.size() > 1) {
                  combined = combiner.reduce(e.getKey(), list.iterator());
               } else {
                  combined = list.get(0);
               }
               l.add(combined);
               result.put(e.getKey(), l);
               log.tracef("For m/r task %s combined %s to %s at %s" , taskId, e.getKey(), combined, cdl.getAddress());
            }
         } finally {
            if (log.isTraceEnabled()) {
               log.tracef("Combine for task %s took %s milliseconds", mcc.getTaskId(),
                          timeService.timeDuration(start, TimeUnit.MILLISECONDS));
            }
            taskLifecycleService.onPostExecute(combiner);
         }
      } else {
         // Combiner not specified
         result = collector.collectedValues();
      }
      return result;
   }

   @Override
   public <T> Map<Address, List<T>> mapKeysToNodes(DistributionManager dm, String taskId,
            Collection<T> keysToMap, boolean useIntermediateCompositeKey) {
      Map<Address, List<T>> addressToKey = new HashMap<Address, List<T>>();
      for (T key : keysToMap) {
         Address ownerOfKey;
         if (useIntermediateCompositeKey) {
            ownerOfKey = dm.getPrimaryLocation(new IntermediateCompositeKey<T>(taskId, key));
         } else {
            ownerOfKey = dm.getPrimaryLocation(key);
         }
         List<T> keysAtNode = addressToKey.get(ownerOfKey);
         if (keysAtNode == null) {
            keysAtNode = new ArrayList<T>();
            addressToKey.put(ownerOfKey, keysAtNode);
         }
         keysAtNode.add(key);
      }
      return addressToKey;
   }

   protected <KIn> Set<KIn> filterLocalPrimaryOwner(Set<KIn> nodeLocalKeys, DistributionManager dm) {
      Set<KIn> selectedKeys = new HashSet<KIn>();
      for (KIn key : nodeLocalKeys) {
         Address primaryLocation = dm != null ? dm.getPrimaryLocation(key) : cdl.getAddress();
         if (primaryLocation != null && primaryLocation.equals(cdl.getAddress())) {
            selectedKeys.add(key);
         }
      }
      return selectedKeys;
   }

   abstract class StatelessDataContainerTask<K,V> implements ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry> {

      V getValue(InternalCacheEntry entry){
         if (entry != null) {
            Object value = entry.getValue();
            if (value instanceof MarshalledValue) {
               value = ((MarshalledValue) value).get();
            }
            return  (V)value;
         } else {
            return null;
         }
      }
  }

   public class IntermediateKeyFilter<T> implements AdvancedCacheLoader.KeyFilter<IntermediateCompositeKey<T>> {

      private final String taskId;

      public IntermediateKeyFilter(String taskId) {
         if (taskId == null || taskId.isEmpty()) {
            throw new IllegalArgumentException("Invalid task Id " + taskId);
         }
         this.taskId = taskId;
      }

     @Override
      public boolean shouldLoadKey(IntermediateCompositeKey<T> key) {
         if (key != null) {
            return taskId.equals(key.getTaskId());
         } else {
            return false;
         }
      }
   }


   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    */
   private static class DefaultCollector<KOut, VOut> implements CollectableCollector<KOut, VOut> {
      
      private final boolean atomicEmit;
      private final Map<KOut, List<VOut>> store;
      
      public DefaultCollector(boolean atomicEmit) {
         this.atomicEmit = atomicEmit;
         store = CollectionFactory.makeConcurrentMap();         
      }

      @Override
      public void emit(KOut key, VOut value) {
         if (atomicEmit) {
            synchronized (this) {
               emitHelper(key, value);
            }
         } else {
            emitHelper(key, value);
         }
      }

      protected void emitHelper(KOut key, VOut value) {
         List<VOut> list = store.get(key);
         if (list == null) {
            list = new LinkedList<VOut>();
            store.put(key, list);
         }
         list.add(value);
      }

      @Override
      public Map<KOut, List<VOut>> collectedValues() {
         return store;
      }
   }

   private interface CollectableCollector<K,V> extends Collector<K, V>{
      Map<K, List<V>> collectedValues();
   }

   private static class DeltaAwareList<E> extends LinkedList<E> implements DeltaAware, Delta{


      /** The serialVersionUID */
      private static final long serialVersionUID = 2176345973026460708L;

      public DeltaAwareList(Collection<? extends E> c) {
         super(c);
      }

      @Override
      public Delta delta() {
         return new DeltaAwareList<E>(this);
      }

      @Override
      public void commit() {
         this.clear();
      }

      @SuppressWarnings("unchecked")
      @Override
      public DeltaAware merge(DeltaAware d) {
         List<E> other = null;
         if (d != null && d instanceof DeltaAwareList) {
            other = (List<E>) d;
            for (E e : this) {
               other.add(e);
            }
            return (DeltaAware) other;
         } else {
            return this;
         }
      }
   }

   /**
    * IntermediateCompositeKey
    *
    */
   public static final class IntermediateCompositeKey<V> implements Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 4434717760740027918L;

      private final String taskId;
      private final V key;

      public IntermediateCompositeKey(String taskId, V key) {
         this.taskId = taskId;
         this.key = key;
      }

      public String getTaskId() {
         return taskId;
      }

      public V getKey(){
         return key;
      }

      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + ((key == null) ? 0 : key.hashCode());
         result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
         return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public boolean equals(Object obj) {
         if (obj == null) {
            return false;
         }
         if (!(obj instanceof IntermediateCompositeKey)) {
            return false;
         }
         IntermediateCompositeKey<V> other = (IntermediateCompositeKey<V>) obj;
         if (key == null) {
            if (other.key != null) {
               return false;
            }
         } else if (!key.equals(other.key)) {
            return false;
         }
         if (taskId == null) {
            if (other.taskId != null) {
               return false;
            }
         } else if (!taskId.equals(other.taskId)) {
            return false;
         }
         return true;
      }

      @Override
      public String toString() {
         return "IntermediateCompositeKey [taskId=" + taskId + ", key=" + key + "]";
      }
   }
}
