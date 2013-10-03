package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
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
   private static final int CANCELLATION_CHECK_FREQUENCY = 32; // Should be a power of two so that the compiler can replace a % with a bitmask
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
   public <KOut, VOut> Map<KOut, VOut> reduce(ReduceCommand<KOut, VOut> reduceCommand)
            throws InterruptedException {
      Cache<?, ?> cache = cacheManager.getCache(reduceCommand.getCacheName());
      Set<KOut> keys = reduceCommand.getKeys();
      String taskId = reduceCommand.getTaskId();
      Reducer<KOut, VOut> reducer = reduceCommand.getReducer();
      boolean useIntermediateKeys = reduceCommand.isEmitCompositeIntermediateKeys();
      boolean noInputKeys = keys == null || keys.isEmpty();
      Cache<Object, List<VOut>> tmpCache = cacheManager.getCache(reduceCommand.getCacheName());
      Map<KOut,VOut> result = new HashMap<KOut, VOut>();
      if (noInputKeys) {
         //illegal state, raise exception
         throw new IllegalStateException("Reduce phase of MapReduceTask " + taskId + " on node "
                  + cdl.getAddress() + " executed with empty input keys");
      } else{
         //first hook into lifecycle
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         log.tracef("For m/r task %s invoking %s at %s",  taskId, reduceCommand, cdl.getAddress());
         int interruptCount = 0;
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            taskLifecycleService.onPreExecute(reducer, cache);
            for (KOut key : keys) {
               interruptCount++;
               if (checkInterrupt(interruptCount++) && Thread.currentThread().isInterrupted())
                  throw new InterruptedException();
               //load result value from map phase
               List<VOut> value;
               if(useIntermediateKeys) {
                  value = tmpCache.get(new IntermediateCompositeKey<KOut>(taskId, key));
               } else {
                  value = tmpCache.get(key);
               }
               // and reduce it
               VOut reduced = reducer.reduce(key, value.iterator());
               result.put(key, reduced);
               log.tracef("For m/r task %s reduced %s to %s at %s ", taskId, key, reduced, cdl.getAddress());
            }
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

   protected <KIn, VIn, KOut, VOut> CollectableCollector<KOut, VOut> map(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      Cache<KIn, VIn> cache = cacheManager.getCache(mcc.getCacheName());
      Set<KIn> keys = mcc.getKeys();
      Set<KIn> inputKeysCopy = null;
      Mapper<KIn, VIn, KOut, VOut> mapper = mcc.getMapper();
      final DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      boolean inputKeysSpecified = keys != null && !keys.isEmpty();
      Set <KIn> inputKeys = keys;
      if (!inputKeysSpecified) {
         inputKeys = filterLocalPrimaryOwner(cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).keySet(), dm);
      } else {
         inputKeysCopy = new HashSet<KIn>(keys);
      }
      // hook map function into lifecycle and execute it
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
      DefaultCollector<KOut, VOut> collector = new DefaultCollector<KOut, VOut>();
      log.tracef("For m/r task %s invoking %s with input keys %s",  mcc.getTaskId(), mcc, inputKeys);
      int interruptCount = 0;
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      try {
         taskLifecycleService.onPreExecute(mapper, cache);
         for (KIn key : inputKeys) {
            if (checkInterrupt(interruptCount++) && Thread.currentThread().isInterrupted())
               throw new InterruptedException();

            VIn value = cache.get(key);
            mapper.map(key, value, collector);
            if (inputKeysSpecified) {
               inputKeysCopy.remove(key);
            }
         }

         if (persistenceManager != null) {
            AdvancedCacheLoader.KeyFilter keyFilter;
            if (inputKeysSpecified) {
               keyFilter = new CollectionKeyFilter(filterLocalPrimaryOwner(inputKeysCopy, dm), true);
            } else {
               keyFilter = new CompositeFilter(new PrimaryOwnerFilter(cdl), new CollectionKeyFilter(inputKeys));
            }
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
      boolean emitCompositeIntermediateKeys = mcc.isEmitCompositeIntermediateKeys();
      Reducer <KOut,VOut> combiner = mcc.getCombiner();
      Set<KOut> mapPhaseKeys = new HashSet<KOut>();
      Cache<Object, DeltaAwareList<VOut>> tmpCache = null;
      if (emitCompositeIntermediateKeys) {
         tmpCache = cacheManager.getCache(DEFAULT_TMP_CACHE_CONFIGURATION_NAME);
      } else {
         tmpCache = cacheManager.getCache(taskId);
      }
      if (tmpCache == null) {
         throw new IllegalStateException("Temporary cache for MapReduceTask " + taskId
                  + " not found on " + cdl.getAddress());
      }
      DistributionManager dm = tmpCache.getAdvancedCache().getDistributionManager();

      if (combiner != null) {
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
         migrateIntermediateKeys(mcc, taskId, emitCompositeIntermediateKeys, mapPhaseKeys, tmpCache, dm, combinedMap);
      } else {
         // Combiner not specified so lets insert each key/uncombined-List pair into tmp cache

         Map<KOut, List<VOut>> collectedValues = collector.collectedValues();
         migrateIntermediateKeys(mcc, taskId, emitCompositeIntermediateKeys, mapPhaseKeys, tmpCache, dm, collectedValues);
      }
      return mapPhaseKeys;
   }

   private <KIn, VIn, KOut, VOut> void migrateIntermediateKeys(MapCombineCommand<KIn, VIn, KOut, VOut> mcc, String taskId, boolean emitCompositeIntermediateKeys, Set<KOut> mapPhaseKeys, Cache<Object, DeltaAwareList<VOut>> tmpCache, DistributionManager dm, Map<KOut, List<VOut>> collectedValues) {
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
                  for (int i = 0; i < values.size(); i += chunkSize) {
                     List<VOut> chunk = values.subList(i, Math.min(values.size(), i + chunkSize));
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

   static boolean checkInterrupt(int counter) {
      return counter % CANCELLATION_CHECK_FREQUENCY == 0;
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

   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    */
   private static class DefaultCollector<KOut, VOut> implements CollectableCollector<KOut, VOut> {

      private final Map<KOut, List<VOut>> store = CollectionFactory.makeConcurrentMap();

      @Override
      public void emit(KOut key, VOut value) {
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

      public DeltaAwareList(E reducedObject) {
         super();
         add(reducedObject);
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
