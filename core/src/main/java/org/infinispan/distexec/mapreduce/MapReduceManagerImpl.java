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
import org.infinispan.commons.util.concurrent.ParallelIterableMap.KeyValueAction;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
      combine(mcc, collector);
      return collector.collectedValues();
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
         final boolean sharedTmpCacheUsed = reduceCommand.isUseIntermediateSharedCache();
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         log.tracef("For m/r task %s invoking %s at %s", taskId, reduceCommand, cdl.getAddress());
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            Cache<IntermediateKey<KOut>, List<VOut>> cache = cacheManager.getCache(reduceCommand.getCacheName());
            taskLifecycleService.onPreExecute(reducer, cache);
            KeyFilter<IntermediateKey<KOut>> filter = new IntermediateKeyFilter<KOut>(taskId, !sharedTmpCacheUsed);
            //iterate all tmp cache entries in memory, do it in parallel
            DataContainer<IntermediateKey<KOut>, List<VOut>> dc = cache.getAdvancedCache().getDataContainer();
            dc.executeTask(filter, new DataContainerTask<IntermediateKey<KOut>, List<VOut>>() {
               @Override
               public void apply(IntermediateKey<KOut> k, InternalCacheEntry<IntermediateKey<KOut>, List<VOut>> v) {
                  KOut key = k.getKey();
                  //resolve Iterable<VOut> for iterated key stored in tmp cache
                  Iterable<VOut> value = getValue(v);
                  if (value == null) {
                     throw new IllegalStateException("Found invalid value in intermediate cache, for key " + key
                           + " during reduce phase execution on " + cacheManager.getAddress() + " for M/R task "
                           + taskId);
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
      int maxCSize = mcc.getMaxCollectorSize();
      final Mapper<KIn, VIn, KOut, VOut> mapper = mcc.getMapper();
      final boolean inputKeysSpecified = keys != null && !keys.isEmpty();

      // hook map function into lifecycle and execute it
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
      final CollectableCollector<KOut, VOut> collector = new SynchronizedCollector<KOut, VOut>(
            new DefaultCollector<KIn, VIn, KOut, VOut>(mcc, maxCSize));
      DataContainer<KIn, VIn> dc = cache.getAdvancedCache().getDataContainer();
      log.tracef("For m/r task %s invoking %s with input keys %s",  mcc.getTaskId(), mcc, keys);
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      try {
         taskLifecycleService.onPreExecute(mapper, cache);
         //User specified input taks keys, most likely a short list of input keys (<10^3), iterate serially
         if (inputKeysSpecified) {
            for (KIn key : keys) {
               VIn value = cache.get(key);
               if (value != null) {
                  mapper.map(key, value, collector);
               }
            }
         } else {
            // here we have to iterate all entries in memory, do it in parallel
            dc.executeTask(new PrimaryOwnerFilter<KIn>(cdl), new DataContainerTask<KIn, VIn>() {
               @Override
               public void apply(KIn key , InternalCacheEntry<KIn, VIn> v) {
                  VIn value = getValue(v);
                  if (value != null) {
                     mapper.map(key, value, collector);
                  }
               }
            });
         }
         // in case we have stores, we have to process key/values from there as well
         if (persistenceManager != null && !inputKeysSpecified) { 
               KeyFilter<?> keyFilter = new CompositeKeyFilter<KIn>(new PrimaryOwnerFilter<KIn>(cdl), new CollectionKeyFilter<KIn>(dc.keySet()));
               persistenceManager.processOnAllStores(keyFilter, new MapReduceCacheLoaderTask<KIn, VIn, KOut, VOut>(mapper, collector),
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
      DataContainer<KIn, VIn>  dc = cache.getAdvancedCache().getDataContainer();
      log.tracef("For m/r task %s invoking %s with input keys %s", mcc.getTaskId(), mcc, mcc.getKeys());
      long start = log.isTraceEnabled() ? timeService.time() : 0;
      final Set<KOut> intermediateKeys = new HashSet<KOut>();
      try {
         taskLifecycleService.onPreExecute(mapper, cache);
         if (inputKeysSpecified) {
            DefaultCollector<KIn, VIn, KOut, VOut> c = new DefaultCollector<KIn, VIn, KOut, VOut>(mcc, maxCSize);
            for (KIn key : keys) {
               VIn value = cache.get(key);
               if (value != null) {
                  mapper.map(key, value, c);
               }
            }
            combine(mcc, c);
            Set<KOut> s = migrateIntermediateKeysAndValues(mcc, c.collectedValues());
            intermediateKeys.addAll(s);
         } else {
            MapCombineTask<KIn, VIn, KOut, VOut> task = new MapCombineTask<KIn, VIn, KOut, VOut>(mcc, maxCSize);
            dc.executeTask(new PrimaryOwnerFilter<KIn>(cdl), task);
            intermediateKeys.addAll(task.getMigratedIntermediateKeys());
            //the last chunk of remaining keys/values to migrate
            Map<KOut, List<VOut>> combinedValues = task.collectedValues();
            Set<KOut> lastOne = migrateIntermediateKeysAndValues(mcc, combinedValues);
            intermediateKeys.addAll(lastOne);
         }

         // in case we have stores, we have to process key/values from there as well
         if (persistenceManager != null && !inputKeysSpecified) {
            KeyFilter<KIn> keyFilter = new CompositeKeyFilter<KIn>(new PrimaryOwnerFilter<KIn>(cdl),
                  new CollectionKeyFilter<KIn>(dc.keySet()));

            MapCombineTask<KIn, VIn, KOut, VOut> task = new MapCombineTask<KIn, VIn, KOut, VOut>(mcc, maxCSize);
            persistenceManager.processOnAllStores(keyFilter, task, true, false);
            intermediateKeys.addAll(task.getMigratedIntermediateKeys());
            //the last chunk of remaining keys/values to migrate
            Map<KOut, List<VOut>> combinedValues =  task.collectedValues();
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

   protected <KIn, VIn, KOut, VOut> void combine(MapCombineCommand<KIn, VIn, KOut, VOut> mcc,
         CollectableCollector<KOut, VOut> c) {
      if (mcc.hasCombiner()) {
         Reducer<KOut, VOut> combiner = mcc.getCombiner();
         Cache<?, ?> cache = cacheManager.getCache(mcc.getCacheName());
         log.tracef("For m/r task %s invoking combiner %s at %s", mcc.getTaskId(), mcc, cdl.getAddress());
         MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService.getInstance();
         long start = log.isTraceEnabled() ? timeService.time() : 0;
         try {
            taskLifecycleService.onPreExecute(combiner, cache);
            for (Entry<KOut, List<VOut>> e : c.collectedValues().entrySet()) {
               List<VOut> mapped = e.getValue();
               if (mapped.size() > 1) {
                   VOut reduced = combiner.reduce(e.getKey(), mapped.iterator());
                   c.emitReduced(e.getKey(), reduced);
               }
            }
         } finally {
            if (log.isTraceEnabled()) {
               log.tracef("Combine for task %s took %s milliseconds", mcc.getTaskId(),
                     timeService.timeDuration(start, TimeUnit.MILLISECONDS));
            }
            taskLifecycleService.onPostExecute(combiner);
         }
      }
   }

   private <KIn, VIn, KOut, VOut> Set<KOut> migrateIntermediateKeysAndValues(
         MapCombineCommand<KIn, VIn, KOut, VOut> mcc, Map<KOut, List<VOut>> collectedValues) {

      String taskId =  mcc.getTaskId();
      String tmpCacheName = mcc.getIntermediateCacheName();
      Cache<IntermediateKey<KOut>, DeltaList<VOut>> tmpCache = cacheManager.getCache(tmpCacheName);
      if (tmpCache == null) {
         throw new IllegalStateException("Temporary cache for MapReduceTask " + taskId
                  + " named " + tmpCacheName + " not found on " + cdl.getAddress());
      }

      Set<KOut> mapPhaseKeys = new HashSet<KOut>();
      DistributionManager dm = tmpCache.getAdvancedCache().getDistributionManager();
      Map<Address, List<KOut>> keysToNodes = mapKeysToNodes(dm, taskId, collectedValues.keySet());
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
                     tmpCache.put(new IntermediateKey<KOut>(taskId, key), delta);
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
            Collection<T> keysToMap) {
      Map<Address, List<T>> addressToKey = new HashMap<Address, List<T>>();
      for (T key : keysToMap) {
         Address ownerOfKey = dm.getPrimaryLocation(new IntermediateKey<T>(taskId, key));
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

   private abstract class DataContainerTask<K,V> implements KeyValueAction<K, InternalCacheEntry<K,V>> {

      @SuppressWarnings("unchecked")
      protected V getValue(InternalCacheEntry<K,V> entry){
         if (entry != null && !entry.isExpired(timeService.wallClockTime())) {
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
   private final class MapCombineTask<K,V, KOut,VOut> extends DataContainerTask<K, V> implements AdvancedCacheLoader.CacheLoaderTask<K,V> {

      private final MapCombineCommand<K, V, KOut, VOut> mcc;
      private final Set<KOut> intermediateKeys;
      private final int queueLimit;
      private final BlockingQueue<DefaultCollector<K, V, KOut, VOut>> queue;

      public MapCombineTask(MapCombineCommand<K, V, KOut, VOut> mcc, int maxCollectorSize) throws Exception {
         super();
         this.queueLimit = Runtime.getRuntime().availableProcessors() * 2;
         this.queue = new ArrayBlockingQueue<DefaultCollector<K, V, KOut, VOut>>(queueLimit + 1);
         this.mcc = mcc;
         this.intermediateKeys = Collections.synchronizedSet(new HashSet<KOut>());
         //fill up queue with collectors
         for (int i = 0; i < queueLimit; i++){
            queue.put(new DefaultCollector<K, V, KOut, VOut>(mcc, maxCollectorSize));
         }
      }

      @Override
      public void apply(K key, InternalCacheEntry<K, V> v) {
         V value = getValue(v);
         if (value != null) {
            try {
               executeMapWithCollector(key, value);
            } catch (InterruptedException e) {
              //reset signal
              Thread.currentThread().interrupt();
            }
         }
      }

      @Override
      public void processEntry(MarshalledEntry<K, V> marshalledEntry, TaskContext taskContext) throws InterruptedException {
         executeMapWithCollector((K)marshalledEntry.getKey(), (V)getValue(marshalledEntry));
      }

      @Override
      @SuppressWarnings("unchecked")
      protected V getValue(InternalCacheEntry<K, V> entry){
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

      private Set<KOut> getMigratedIntermediateKeys() {
         return intermediateKeys;
      }

      private Map<KOut, List<VOut>> collectedValues() {
         //combine all collectors from the queue into one
         DefaultCollector<K, V, KOut, VOut> finalCollector = new DefaultCollector<K, V, KOut, VOut>(mcc, Integer.MAX_VALUE);
         for (DefaultCollector<K, V, KOut, VOut> collector : queue) {
            if (!collector.isEmpty()) {
               finalCollector.emit(collector.collectedValues());
               collector.reset();
            }
         }
         combine(mcc, finalCollector);
         return finalCollector.collectedValues();
      }

      private void executeMapWithCollector(K key, V value) throws InterruptedException {
         DefaultCollector<K, V, KOut, VOut> c = null;
         try {
            // grab collector C from the bounded queue
            c = queue.take();
            //invoke mapper with collector C
            mcc.getMapper().map((K) key, value, c);
            migrate(c);
         } finally {
            queue.put(c);
         }
      }

      private void migrate(final DefaultCollector<K, V, KOut, VOut> c) {
         // if overflow even after combine then migrate these keys/values
         if (c.isOverflown()) {
            Set<KOut> migratedKeys = migrateIntermediateKeysAndValues(mcc, c.collectedValues());
            intermediateKeys.addAll(migratedKeys);
            c.reset();
         }
      }

      @SuppressWarnings("unchecked")
      private V getValue(MarshalledEntry<K, V> marshalledEntry) {
         Object loadedValue = marshalledEntry.getValue();
         if (loadedValue instanceof MarshalledValue) {
            return  (V) ((MarshalledValue) loadedValue).get();
         } else {
            return (V) loadedValue;
         }
      }
   }

   private static final class IntermediateKeyFilter<T> implements KeyFilter<IntermediateKey<T>> {

      private final String taskId;
      private final boolean acceptAll;

      public IntermediateKeyFilter(String taskId, boolean acceptAll) {
         if (taskId == null || taskId.isEmpty()) {
            throw new IllegalArgumentException("Invalid task Id " + taskId);
         }
         this.taskId = taskId;
         this.acceptAll = acceptAll;
      }

     @Override
      public boolean accept(IntermediateKey<T> key) {
         if (acceptAll) {
            return true;
         } else {
            if (key != null) {
               return taskId.equals(key.getTaskId());
            } else {
               return false;
            }
         }
      }
   }

   /**
    * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
    * @author Dan Berindei
    * @author William Burns
    * @author Vladimir Blagojevic
    */
   private final class DefaultCollector<K, V, KOut, VOut> implements CollectableCollector<KOut, VOut> {

      private Map<KOut, List<VOut>> store;
      private final AtomicInteger emitCount;
      private final int maxCollectorSize;
      private MapCombineCommand<K, V, KOut, VOut> mcc;

      public DefaultCollector(MapCombineCommand<K, V, KOut, VOut> mcc, int maxCollectorSize) {
         store = new HashMap<KOut, List<VOut>>(1024, 0.75f);
         emitCount = new AtomicInteger();
         this.maxCollectorSize = maxCollectorSize;
         this.mcc = mcc;
      }

      @Override
      public void emit(KOut key, VOut value) {
         List<VOut> list = store.get(key);
         if (list == null) {
            list = new ArrayList<VOut>(128);
            store.put(key, list);
         }
         list.add(value);
         emitCount.incrementAndGet();
         if (isOverflown() && mcc.hasCombiner()) {
            combine(mcc, this);
         }
      }

      public void emitReduced(KOut key, VOut value) {
         List<VOut> list = store.get(key);
         int prevSize = list.size();
         list.clear();
         list.add(value);
         //we remove prevSize elements and replace it with one (the reduced value)
         emitCount.addAndGet(-prevSize + 1);
      }

      @Override
      public Map<KOut, List<VOut>> collectedValues() {
         return store;
      }

      public void reset(){
         store.clear();
         emitCount.set(0);
      }

      public boolean isEmpty() {
         return store.isEmpty();
      }

      public void emit(Map<KOut, List<VOut>> combined) {
         for (Entry<KOut, List<VOut>> e : combined.entrySet()) {
            KOut k = e.getKey();
            List<VOut> values = e.getValue();
            for (VOut v : values) {
               emit(k, v);
            }
         }
      }

      public boolean isOverflown() {
         return emitCount.get() > maxCollectorSize;
      }
   }

   private interface CollectableCollector<K,V> extends Collector<K, V>{
      Map<K, List<V>> collectedValues();
      void emitReduced(K key, V value);
   }

   private final class SynchronizedCollector<KOut, VOut> implements CollectableCollector<KOut, VOut> {

      private CollectableCollector<KOut, VOut> delegate;

      public SynchronizedCollector(CollectableCollector<KOut, VOut> delegate) {
         this.delegate = delegate;
      }

      @Override
      public synchronized void emit(KOut key, VOut value) {
         delegate.emit(key, value);
      }

      public synchronized void emitReduced(KOut key, VOut value) {
         delegate.emitReduced(key, value);
      }

      @Override
      public synchronized Map<KOut, List<VOut>> collectedValues() {
         return delegate.collectedValues();
      }
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

   @SuppressWarnings("rawtypes")
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

   @SuppressWarnings("rawtypes")
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
   public static final class IntermediateKey<V> implements Serializable {

      /** The serialVersionUID */
      private static final long serialVersionUID = 4434717760740027918L;

      private final String taskId;
      private final V key;

      public IntermediateKey(String taskId, V key) {
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
         if (!(obj instanceof IntermediateKey)) {
            return false;
         }
         IntermediateKey<V> other = (IntermediateKey<V>) obj;
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
