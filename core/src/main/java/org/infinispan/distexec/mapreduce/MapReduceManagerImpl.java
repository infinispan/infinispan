package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.PrimaryOwnerFilter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
      Map<KOut, List<VOut>> collectedValues = collector.removeCollectedValues();
      return combine(mcc, collectedValues);
   }

   @Override
   public <KIn, VIn, KOut, VOut> Set<KOut> mapAndCombineForDistributedReduction(
            MapCombineCommand<KIn, VIn, KOut, VOut> mcc) throws InterruptedException {
      try {
         return mapAndCombine(mcc);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   public <KOut, VOut> Map<KOut, VOut> reduce(ReduceCommand<KOut, VOut> reduceCommand) throws InterruptedException {
      final Map<KOut, VOut> result = CollectionFactory.makeConcurrentMap(256);
      reduce(reduceCommand, result);
      return result;
   }

   @Override
   public <KOut, VOut> void reduce(ReduceCommand<KOut, VOut> reduceCommand, String resultCache) throws InterruptedException{
      Cache<KOut, VOut> cache = cacheManager.getCache(resultCache);
      reduce(reduceCommand, cache);
   }

   @SuppressWarnings("unchecked")
   protected <KOut, VOut> void reduce(ReduceCommand<KOut, VOut> reduceCommand, final Map<KOut, VOut> result)
         throws InterruptedException {
      final Set<KOut> keys = reduceCommand.getKeys();
      final String taskId = reduceCommand.getTaskId();
      boolean noInputKeys = keys == null || keys.isEmpty();

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
            KeyFilter<?> filter;
            if (useIntermediateKeys) {
               //shared intermediate cache, filter keys that belong to this task
               filter = new IntermediateKeyFilter<KOut>(taskId);
            } else {
               //dedicated tmp cache, all keys belong to this task
               filter = KeyFilter.LOAD_ALL_FILTER;
            }
            //iterate all tmp cache entries in memory, do it in parallel
            DataContainer dc = cache.getAdvancedCache().getDataContainer();
            dc.executeTask(filter, new DataContainerTask<KOut, List<VOut>>() {
               @Override
               public void apply(Object k, InternalCacheEntry v) {
                  KOut key = null;
                  if (useIntermediateKeys) {
                     IntermediateCompositeKey<KOut> intKey = (IntermediateCompositeKey<KOut>) k;
                     key = intKey.getKey();
                  } else {
                     key = (KOut) k;
                  }
                  //resolve Iterable<VOut> for iterated key stored in tmp cache
                  Iterable<VOut> value = getValue(v);
                  if (value == null) {
                     throw new IllegalStateException("Found invalid value in intermediate cache, for key " + key
                           + " during reduce phase execution on " + cacheManager.getAddress() + " for M/R task " + taskId);
                  }
                  // and reduce it
                  VOut reduced = reducer.reduce(key, value.iterator());
                  result.put(key, reduced);
                  log.tracef("For m/r task %s reduced %s to %s at %s ", taskId, key, reduced, cdl.getAddress());
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
            dc.executeTask(new PrimaryOwnerFilter(cdl), new DataContainerTask<KIn, VIn>() {
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
               KeyFilter<?> keyFilter = new CompositeKeyFilter(new PrimaryOwnerFilter(cdl), new CollectionKeyFilter(dc.keySet()));
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

   @SuppressWarnings("unchecked")
   protected <KIn, VIn, KOut, VOut> Set<KOut> mapAndCombine(final MapCombineCommand<KIn, VIn, KOut, VOut> mcc)
         throws Exception {

      final Cache<KIn, VIn> cache = cacheManager.getCache(mcc.getCacheName());
      Set<KIn> keys = mcc.getKeys();
      int maxCSize = mcc.getMaxCollectorSize();
      final Mapper<KIn, VIn, KOut, VOut> mapper = mcc.getMapper();
      final boolean inputKeysSpecified = keys != null && !keys.isEmpty();
      // hook map function into lifecycle and execute it
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
      final DefaultCollector<KOut, VOut> c = new DefaultCollector<KOut, VOut>(maxCSize, !inputKeysSpecified);
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      log.tracef("For m/r task %s invoking %s with input keys %s", mcc.getTaskId(), mcc, mcc.getKeys());
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      final Set<KOut> intermediateKeys = Collections.synchronizedSet(new HashSet<KOut>());
      try {
         taskLifecycleService.onPreExecute(mapper, cache);
         if (inputKeysSpecified) {
            for (KIn key : keys) {
               VIn value = cache.get(key);
               mapper.map(key, value, c);
            }
            Map<KOut, List<VOut>> combinedValues = combine(mcc, c.removeCollectedValues());
            Set<KOut> s = migrateIntermediateKeysAndValues(mcc, combinedValues);
            intermediateKeys.addAll(s);
         } else {
            MapCombineTask<KIn, VIn, KOut, VOut> task = new MapCombineTask<KIn, VIn, KOut, VOut>(c, mcc, maxCSize);
            dc.executeTask(new PrimaryOwnerFilter(cdl), task);
            intermediateKeys.addAll(task.getIntermediateKeys());

            //the remaining last chunk from collector
            Map<KOut, List<VOut>> combinedValues = combine(mcc, c.removeCollectedValues());
            Set<KOut> lastOne = migrateIntermediateKeysAndValues(mcc, combinedValues);
            intermediateKeys.addAll(lastOne);
         }

         // in case we have stores, we have to process key/values from there as well
         if (persistenceManager != null && !inputKeysSpecified) {
            final DefaultCollector<KOut, VOut> pmc = new DefaultCollector<KOut, VOut>(maxCSize, true);
            KeyFilter<?> keyFilter = new CompositeKeyFilter(new PrimaryOwnerFilter(cdl),
                  new CollectionKeyFilter(dc.keySet()));

            MapCombineTask<KIn, VIn, KOut, VOut> task = new MapCombineTask<KIn, VIn, KOut, VOut>(pmc, mcc, maxCSize);
            persistenceManager.processOnAllStores(keyFilter, task, true, false);
            intermediateKeys.addAll(task.getIntermediateKeys());

            //the remaining last chunk from store collector
            Map<KOut, List<VOut>> combinedValues = combine(mcc, pmc.removeCollectedValues());
            Set<KOut> lastOne = migrateIntermediateKeysAndValues(mcc, combinedValues);
            intermediateKeys.addAll(lastOne);
         }
      } finally {
         if (log.isTraceEnabled()) {
            log.tracef("Map phase for task %s took %s milliseconds", mcc.getTaskId(),
                  timeService.timeDuration(start, TimeUnit.MILLISECONDS));
         }
         taskLifecycleService.onPostExecute(mapper);
      }
      return intermediateKeys;
   }

   protected <KIn, VIn, KOut, VOut> Map<KOut, List<VOut>> combine(MapCombineCommand<KIn, VIn, KOut, VOut> mcc,
                                                                  Map<KOut, List<VOut>> collectedValues) {
      Map<KOut, List<VOut>> combinedMap = null;
      if (mcc.hasCombiner()) {
         combinedMap = new HashMap<KOut, List<VOut>>();
         Reducer<KOut, VOut> combiner = mcc.getCombiner();
         Cache<?, ?> cache = cacheManager.getCache(mcc.getCacheName());
         log.tracef("For m/r task %s invoking combiner %s at %s", mcc.getTaskId(), mcc, cdl.getAddress());
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            taskLifecycleService.onPreExecute(combiner, cache);
            for (Entry<KOut, List<VOut>> e : collectedValues.entrySet()) {
               List<VOut> mapped = e.getValue();
               List<VOut> combined;
               if (mapped.size() == 1) {
                  combined = mapped;
               } else {
                  combined = Arrays.asList(combiner.reduce(e.getKey(), mapped.iterator()));
               }
               combinedMap.put(e.getKey(), combined);
               log.tracef("For m/r task %s combined %s to %s at %s", mcc.getTaskId(), e.getKey(), combined,
                     cdl.getAddress());
            }
         } finally {
            if (log.isTraceEnabled()) {
               log.tracef("Combine for task %s took %s milliseconds", mcc.getTaskId(),
                     timeService.timeDuration(start, TimeUnit.MILLISECONDS));
            }
            taskLifecycleService.onPostExecute(combiner);
         }
      } else {
         // Combiner not specified so lets insert each key/uncombined-List pair into tmp cache
         combinedMap = collectedValues;
      }
      return combinedMap;
   }

   private <KIn, VIn, KOut, VOut> Set<KOut> migrateIntermediateKeysAndValues(
         MapCombineCommand<KIn, VIn, KOut, VOut> mcc, Map<KOut, List<VOut>> collectedValues) {

      String taskId =  mcc.getTaskId();
      String tmpCacheName = mcc.getIntermediateCacheName();
      Cache<Object, DeltaList<VOut>> tmpCache = cacheManager.getCache(tmpCacheName);
      if (tmpCache == null) {
         throw new IllegalStateException("Temporary cache for MapReduceTask " + taskId
                  + " named " + tmpCacheName + " not found on " + cdl.getAddress());
      }

      Set<KOut> mapPhaseKeys = new HashSet<KOut>();
      DistributionManager dm = tmpCache.getAdvancedCache().getDistributionManager();
      boolean emitCompositeIntermediateKeys = mcc.isEmitCompositeIntermediateKeys();
      Map<Address, List<KOut>> keysToNodes = mapKeysToNodes(dm, taskId, collectedValues.keySet(),
            emitCompositeIntermediateKeys);
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      tmpCache = tmpCache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      try {
         for (Entry<Address, List<KOut>> entry : keysToNodes.entrySet()) {
            List<KOut> keysHashedToAddress = entry.getValue();
            try {
               log.tracef("For m/r task %s migrating intermediate keys %s to %s", taskId, keysHashedToAddress, entry.getKey());
               for (KOut key : keysHashedToAddress) {
                  List<VOut> values = collectedValues.get(key);
                  int entryTransferCount = chunkSize;
                  for (int i = 0; i < values.size(); i += entryTransferCount) {
                     List<VOut> chunk = values.subList(i, Math.min(values.size(), i + entryTransferCount));
                     DeltaList<VOut> delta = new DeltaList<VOut>(chunk);
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
      return mapPhaseKeys;
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

   private abstract class DataContainerTask<K,V> implements ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry> {

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

   /**
    * This is the parallel staggered map/combine algorithm. Threads from the default fork/join pool
    * traverse container and store key/value pairs in parallel. As one of the threads hits the
    * maxCollectorSize threshold, it takes the snapshot of the current state of the collector and
    * invokes combine on it all while others threads continue to fill up collector up to the point
    * where the threshold is reached again. The thread that broke the collector threshold invokes
    * combine and the algorithm repeats. The benefit of staggered parallel map/combine is manyfold.
    * First, we never exhaust working memory of a node as we batch map/combine execution all while
    * traversal of key/value pairs is in progress. Second, such a staggered combine execution does
    * not cause underlying transport to be completely saturated by intermediate cache put commands;
    * intermediate key/value pairs of map/reduce algorithm are transferred across the cluster
    * smoothly as parallel traversal of container's key/value pairs is progress.
    *
    */
   private final class MapCombineTask<K,V, KOut,VOut> extends DataContainerTask<K, V> implements AdvancedCacheLoader.CacheLoaderTask {

      DefaultCollector<KOut, VOut> collector;
      MapCombineCommand<K, V, KOut, VOut> mcc;
      Set<KOut> intermediateKeys;
      int maxCollectorSize;

      public MapCombineTask(DefaultCollector<KOut, VOut> collector,
            MapCombineCommand<K, V, KOut, VOut> mcc, int maxCollectorSize) {
         super();
         this.collector = collector;
         this.mcc = mcc;
         this.intermediateKeys = Collections.synchronizedSet(new HashSet<KOut>());
         this.maxCollectorSize = maxCollectorSize;
      }

      public Set<KOut> getIntermediateKeys() {
         return intermediateKeys;
      }

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

      @Override
      public void apply(Object key, InternalCacheEntry v) {
         V value = getValue(v);
         if (value != null) {
            mcc.getMapper().map((K) key, value, collector);
         }
         combineAndMigrate();
      }

      protected void combineAndMigrate() throws CacheException {
         if (collector.size() > maxCollectorSize) {
            final Map<KOut, List<VOut>> batch = collector.removeCollectedValues();
            if (!batch.isEmpty()) {
               Map<KOut, List<VOut>> combinedValues = combine(mcc, batch);
               Set<KOut> migratedKeys = migrateIntermediateKeysAndValues(mcc, combinedValues);
               intermediateKeys.addAll(migratedKeys);
            }
         }
      }

      @Override
      public void processEntry(MarshalledEntry marshalledEntry, TaskContext taskContext) throws InterruptedException {
         mcc.getMapper().map((K)marshalledEntry.getKey(), (V)getValue(marshalledEntry), collector);
         combineAndMigrate();
      }

      private Object getValue(MarshalledEntry marshalledEntry) {
         Object loadedValue = marshalledEntry.getValue();
         if (loadedValue instanceof MarshalledValue) {
            return  ((MarshalledValue) loadedValue).get();
         } else {
            return loadedValue;
         }
      }
   }

   private static final class IntermediateKeyFilter<T> implements KeyFilter<IntermediateCompositeKey<T>> {

      private final String taskId;

      public IntermediateKeyFilter(String taskId) {
         if (taskId == null || taskId.isEmpty()) {
            throw new IllegalArgumentException("Invalid task Id " + taskId);
         }
         this.taskId = taskId;
      }

     @Override
      public boolean accept(IntermediateCompositeKey<T> key) {
         if (key != null) {
            return taskId.equals(key.getTaskId());
         } else {
            return false;
         }
      }
   }

   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    * @author Dan Berindei
    * @author William Burns
    * @author Vladimir Blagojevic
    */
   private static final class DefaultCollector<KOut, VOut> implements CollectableCollector<KOut, VOut> {

      private final boolean atomicEmit;
      private Map<KOut, List<VOut>> store;
      private final AtomicInteger emitCount;

      public DefaultCollector(int size, boolean atomicEmit) {
         this.atomicEmit = atomicEmit;
         store = new LinkedHashMap<KOut, List<VOut>>(size, 0.75f, true);
         emitCount = new AtomicInteger();
      }

      public DefaultCollector(boolean atomicEmit) {
         this(128, atomicEmit);
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
         emitCount.incrementAndGet();
      }

      @Override
      public Map<KOut, List<VOut>> removeCollectedValues() {
         HashMap<KOut, List<VOut>> values;
         synchronized (this) {
            values = new HashMap<KOut, List<VOut>>(store);
            store.clear();
            emitCount.set(0);
         }
         return values;
      }

      @Override
      public Map<KOut, List<VOut>> removeLRUEntry() {
         Map<KOut, List<VOut>> lrus = Collections.emptyMap();
         synchronized (this) {
            Iterator<KOut> iterator = store.keySet().iterator();
            if (iterator.hasNext()){
               KOut out = iterator.next();
               List<VOut> list = store.remove(out);
               lrus = Collections.singletonMap(out, list);
               emitCount.addAndGet(-list.size());
            }
         }
         return lrus;
      }

      public int size() {
         return emitCount.get();
      }
   }

   private interface CollectableCollector<K,V> extends Collector<K, V>{
      Map<K, List<V>> removeCollectedValues();
      Map<K, List<V>> removeLRUEntry();
   }

   private static class DeltaAwareList<E> implements Iterable<E>, DeltaAware {

      private final List<E> list;

      public DeltaAwareList(List<E> list) {
         this.list = list;
      }

      @Override
      public Delta delta() {
         return new DeltaList<E>(list);
      }

      @Override
      public void commit() {
         list.clear();
      }

      @Override
      public Iterator<E> iterator(){
         return list.iterator();
      }
   }

   private static class DeltaList<E> implements Delta {

      private final List<E> deltas;

      public DeltaList(List<E> list) {
         deltas = new ArrayList<E>(list);
      }

      @SuppressWarnings("unchecked")
      @Override
      public DeltaAware merge(DeltaAware d) {
         DeltaAwareList<E> other = null;
         if (d instanceof DeltaAwareList) {
            other = (DeltaAwareList<E>) d;
            other.list.addAll(deltas);
         } else {
            other = new DeltaAwareList<E>(deltas);
         }
         return other;
      }
   }

   public static class DeltaListExternalizer extends AbstractExternalizer<DeltaList> {

      private static final long serialVersionUID = 5859147782602054109L;

      @Override
      public void writeObject(ObjectOutput output, DeltaList list) throws IOException {
         output.writeObject(list.deltas);
      }

      @Override
      @SuppressWarnings("unchecked")
      public DeltaList readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new DeltaList((List) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.DELTA_MAPREDUCE_LIST_ID;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends DeltaList>> getTypeClasses() {
         return Util.<Class<? extends DeltaList>>asSet(DeltaList.class);
      }
   }

   public static class DeltaAwareListExternalizer extends AbstractExternalizer<DeltaAwareList> {

      private static final long serialVersionUID = -8956663669844107351L;

      @Override
      public void writeObject(ObjectOutput output, DeltaAwareList deltaAwareList) throws IOException {
         output.writeObject(deltaAwareList.list);
      }

      @Override
      @SuppressWarnings("unchecked")
      public DeltaAwareList readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new DeltaAwareList((List) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.DELTA_AWARE_MAPREDUCE_LIST_ID;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends DeltaAwareList>> getTypeClasses() {
         return Util.<Class<? extends DeltaAwareList>>asSet(DeltaAwareList.class);
      }
   }

   /**
    * IntermediateCompositeKey
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
