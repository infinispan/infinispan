package org.infinispan.distexec.mapreduce;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.CancelCommand;
import org.infinispan.commands.CancellationService;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.distexec.mapreduce.MapReduceManagerImpl.IntermediateKey;
import org.infinispan.distexec.mapreduce.spi.MapReduceTaskLifecycleService;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

/**
 * MapReduceTask is a distributed task allowing a large scale computation to be transparently
 * parallelized across Infinispan cluster nodes.
 * <p>
 *
 * Users should instantiate MapReduceTask with a reference to a cache whose data is used as input for this
 * task. Infinispan execution environment will migrate and execute instances of provided {@link Mapper}
 * and {@link Reducer} seamlessly across Infinispan nodes.
 * <p>
 *
 * Unless otherwise specified using {@link MapReduceTask#onKeys(Object...)} filter all available
 * key/value pairs of a specified cache will be used as input data for this task.
 *
 * For example, MapReduceTask that counts number of word occurrences in a particular cache where
 * keys and values are String instances could be written as follows:
 *
 * <pre>
 * MapReduceTask&lt;String, String, String, Integer&gt; task = new MapReduceTask&lt;String, String, String, Integer&gt;(cache);
 * task.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer());
 * Map&lt;String, Integer&gt; results = task.execute();
 * </pre>
 *
 * The final result is a map where key is a word and value is a word count for that particular word.
 * <p>
 *
 * Accompanying {@link Mapper} and {@link Reducer} are defined as follows:
 *
 * <pre>
 *    private static class WordCountMapper implements Mapper&lt;String, String, String,Integer&gt; {
 *
 *     public void map(String key, String value, Collector&lt;String, Integer&gt; collector) {
 *        StringTokenizer tokens = new StringTokenizer(value);
 *       while (tokens.hasMoreElements()) {
 *           String s = (String) tokens.nextElement();
 *           collector.emit(s, 1);
 *        }
 *     }
 *  }
 *
 *   private static class WordCountReducer implements Reducer&lt;String, Integer&gt; {
 *
 *      public Integer reduce(String key, Iterator&lt;Integer&gt; iter) {
 *         int sum = 0;
 *         while (iter.hasNext()) {
 *            Integer i = (Integer) iter.next();
 *            sum += i;
 *        }
 *         return sum;
 *      }
 *   }
 * </pre>
 *
 * <p>
 *
 * Finally, as of Infinispan 5.2 release, MapReduceTask can also specify a Combiner function. The Combiner
 * is executed on each node after the Mapper and before the global reduce phase. The Combiner receives input from
 * the Mapper's output and the output from the Combiner is then sent to the reducers. It is useful to think
 * of the Combiner as a node local reduce phase before global reduce phase is executed.
 * <p>
 *
 * Combiners are especially useful when reduce function is both commutative and associative! In such cases
 * we can use the Reducer itself as the Combiner; all one needs to do is to specify the Combiner:
 * <pre>
 * MapReduceTask&lt;String, String, String, Integer&gt; task = new MapReduceTask&lt;String, String, String, Integer&gt;(cache);
 * task.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer()).combineWith(new WordCountReducer());
 * Map&lt;String, Integer&gt; results = task.execute();
 * </pre>
 *
 * Note that {@link Mapper} and {@link Reducer} should not be specified as inner classes. Inner classes
 * declared in non-static contexts contain implicit non-transient references to enclosing class instances,
 * serializing such an inner class instance will result in serialization of its associated outer class instance as well.
 *
 * <p>
 *
 * If you are not familiar with concept of map reduce distributed execution model
 * start with Google's MapReduce research <a href="http://labs.google.com/papers/mapreduce.html">paper</a>.
 *
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Sanne Grinovero
 *
 * @since 5.0
 */
public class MapReduceTask<KIn, VIn, KOut, VOut> {

   private static final Log log = LogFactory.getLog(MapReduceTask.class);
   public static final String DEFAULT_TMP_CACHE_CONFIGURATION_NAME= "__tmpMapReduce";

   protected Mapper<KIn, VIn, KOut, VOut> mapper;
   protected Reducer<KOut, VOut> reducer;
   protected Reducer<KOut, VOut> combiner;
   protected final boolean distributeReducePhase;
   protected boolean useIntermediateSharedCache;

   protected final Collection<KIn> keys;
   protected final AdvancedCache<KIn, VIn> cache;
   protected final Marshaller marshaller;
   protected final MapReduceManager mapReduceManager;
   protected final CancellationService cancellationService;
   protected final List<CancellableTaskPart> cancellableTasks;
   protected final UUID taskId;
   protected final ClusteringDependentLogic clusteringDependentLogic;
   protected final boolean isLocalOnly;
   protected RpcOptionsBuilder rpcOptionsBuilder;
   protected String customIntermediateCacheName;
   protected String intermediateCacheConfigurationName = DEFAULT_TMP_CACHE_CONFIGURATION_NAME;
   private StateTransferManager stateTransferManager;
   private static final int MAX_COLLECTOR_SIZE = 1000;

   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node. This task will by default only use distributed map phase while
    * reduction will be executed on task originating Infinispan node.
    * <p>
    *
    * Large and data intensive tasks whose reduction phase would exceed working memory of one
    * Infinispan node should use distributed reduce phase
    *
    * @param masterCacheNode
    *           cache node initiating map reduce task
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode) {
      this(masterCacheNode, false, false);
   }

   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node.
    *
    * @param masterCacheNode
    *           cache node initiating map reduce task
    * @param distributeReducePhase
    *           if true this task will use distributed reduce phase execution
    *
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode, boolean distributeReducePhase) {
      this(masterCacheNode, distributeReducePhase, true);
   }


   /**
    * Create a new MapReduceTask given a master cache node. All distributed task executions will be
    * initiated from this cache node.
    *
    * @param masterCacheNode
    *           cache node initiating map reduce task
    * @param distributeReducePhase
    *           if true this task will use distributed reduce phase execution
    * @param useIntermediateSharedCache
    *           if true this tasks will share intermediate value cache with other executing
    *           MapReduceTasks on the grid. Otherwise, if false, this task will use its own
    *           dedicated cache for intermediate values
    *
    */
   public MapReduceTask(Cache<KIn, VIn> masterCacheNode, boolean distributeReducePhase, boolean useIntermediateSharedCache) {
      if (masterCacheNode == null)
         throw new IllegalArgumentException("Can not use null cache for MapReduceTask");
      ensureAccessPermissions(masterCacheNode.getAdvancedCache());
      ensureProperCacheState(masterCacheNode.getAdvancedCache());
      this.cache = masterCacheNode.getAdvancedCache();
      this.keys = new LinkedList<KIn>();
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache);
      this.marshaller = componentRegistry.getComponent(StreamingMarshaller.class, CACHE_MARSHALLER);
      this.mapReduceManager = componentRegistry.getComponent(MapReduceManager.class);
      this.cancellationService = componentRegistry.getComponent(CancellationService.class);
      this.stateTransferManager = componentRegistry.getComponent(StateTransferManager.class);
      this.taskId = UUID.randomUUID();
      if (useIntermediateSharedCache) {
         this.customIntermediateCacheName = DEFAULT_TMP_CACHE_CONFIGURATION_NAME;
      } else {
         this.customIntermediateCacheName = taskId.toString();
      }
      this.distributeReducePhase = distributeReducePhase;
      this.useIntermediateSharedCache = useIntermediateSharedCache;
      this.cancellableTasks = Collections.synchronizedList(new ArrayList<CancellableTaskPart>());
      this.clusteringDependentLogic = componentRegistry.getComponent(ClusteringDependentLogic.class);
      this.isLocalOnly = SecurityActions.getCacheRpcManager(cache) == null;
      this.rpcOptionsBuilder = isLocalOnly ? null : new RpcOptionsBuilder(SecurityActions.getCacheRpcManager(cache).getDefaultRpcOptions(true));
      if (!isLocalOnly) {
         this.rpcOptionsBuilder.timeout(0, TimeUnit.MILLISECONDS);
      }
   }

   /**
    * Rather than use all available keys as input <code>onKeys</code> allows users to specify a
    * subset of keys as input to this task
    *
    * @param input
    *           input keys for this task
    * @return this task
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> onKeys(KIn... input) {
      Collections.addAll(keys, input);
      return this;
   }

   /**
    * Specifies Mapper to use for this MapReduceTask
    * <p>
    * Note that {@link Mapper} should not be specified as inner class. Inner classes declared in
    * non-static contexts contain implicit non-transient references to enclosing class instances,
    * serializing such an inner class instance will result in serialization of its associated outer
    * class instance as well.
    *
    * @param mapper used to execute map phase of MapReduceTask
    * @return this MapReduceTask itself
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> mappedWith(Mapper<KIn, VIn, KOut, VOut> mapper) {
      if (mapper == null)
         throw new IllegalArgumentException("A valid reference of Mapper is needed");
      this.mapper = mapper;
      return this;
   }

   /**
    * Specifies Reducer to use for this MapReduceTask
    *
    * <p>
    * Note that {@link Reducer} should not be specified as inner class. Inner classes declared in
    * non-static contexts contain implicit non-transient references to enclosing class instances,
    * serializing such an inner class instance will result in serialization of its associated outer
    * class instance as well.
    *
    * @param reducer used to reduce results of map phase
    * @return this MapReduceTask itself
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> reducedWith(Reducer<KOut, VOut> reducer) {
      if (reducer == null)
         throw new IllegalArgumentException("A valid reference of Reducer is needed");
      this.reducer = reducer;
      return this;
   }

   /**
    * Specifies Combiner to use for this MapReduceTask
    *
    * <p>
    * Note that {@link Reducer} should not be specified as inner class. Inner classes declared in
    * non-static contexts contain implicit non-transient references to enclosing class instances,
    * serializing such an inner class instance will result in serialization of its associated outer
    * class instance as well.
    *
    * @param combiner used to immediately combine results of map phase before reduce phase is invoked
    * @return this MapReduceTask itself
    * @since 5.2
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> combinedWith(Reducer<KOut, VOut> combiner) {
      if (combiner == null)
         throw new IllegalArgumentException("A valid reference of Reducer/Combiner is needed");
      this.combiner = combiner;
      return this;
   }

   /**
    * See {@link #timeout(TimeUnit)}.
    *
    * Note: the timeout value will be converted to milliseconds and a value less or equal than zero means wait forever.
    * The default timeout for this task is 0. The task will wait indefinitely for its completion.
    *
    * @param timeout
    * @param unit
    * @return this MapReduceTask itself
    */
   public final MapReduceTask<KIn, VIn, KOut, VOut> timeout(long timeout, TimeUnit unit) {
      rpcOptionsBuilder.timeout(timeout, unit);
      return this;
   }

   /**
    * @return the timeout value in {@link TimeUnit} to wait for the remote map/reduce task to finish. The default
    * timeout is 0, the task will wait indefinitely for its completion.
    */
   public final long timeout(TimeUnit outputTimeUnit) {
      return rpcOptionsBuilder.timeout(outputTimeUnit);
   }

   /**
    *
    * Allows this MapReduceTask to use specific intermediate custom defined cache for storage of
    * intermediate <KOut, List<VOut>> key/values pairs. Intermediate cache is used to store output
    * of map phase and it is not shared with other M/R tasks. Upon completion of M/R task this intermediate
    * cache is destroyed.
    *
    * @param cacheConfigurationName
    *           name of the cache configuration to use for the intermediate cache
    * @return this MapReduceTask iteself
    * @since 7.0
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> usingIntermediateCache(String cacheConfigurationName) {
      if (cacheConfigurationName == null || cacheConfigurationName.isEmpty()) {
         throw new IllegalArgumentException("Invalid configuration name " + cacheConfigurationName
               + ", cacheConfigurationName cannot be null or empty");
      }
      this.intermediateCacheConfigurationName = cacheConfigurationName;
      this.useIntermediateSharedCache = false;
      return this;
   }

   /**
    * Allows this MapReduceTask to use a specific shared intermediate cache for storage of
    * intermediate <KOut, List<VOut>> key/values pairs. Intermediate shared cache is used to store
    * output of map phase and it is shared with other M/R tasks that also specify a shared
    * intermediate cache with the same name.
    *
    * @param cacheName
    *           name of the custom cache
    * @return this MapReduceTask iteself
    * @since 7.0
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> usingSharedIntermediateCache(String cacheName) {
      if (cacheName == null || cacheName.isEmpty()) {
         throw new IllegalArgumentException("Invalid cache name" + cacheName + ", cache name cannot be null or empty");
      }
      this.customIntermediateCacheName = cacheName;
      this.useIntermediateSharedCache = true;
      return this;
   }

   /**
    * Allows this MapReduceTask to use a specific shared intermediate cache for storage of
    * intermediate <KOut, List<VOut>> key/values pairs. Intermediate shared cache is used to store
    * output of map phase and it is shared with other M/R tasks that also specify a shared
    * intermediate cache with the same name.
    * <p>
    * Rather than using MapReduceTask default configuration for intermediate cache this method
    * allows clients to specify custom shared cache configuration.
    *
    *
    * @param cacheName
    *           name of the custom cache
    * @param cacheConfigurationName
    *           name of the cache configuration to use for the intermediate cache
    * @return this MapReduceTask iteself
    * @since 7.0
    */
   public MapReduceTask<KIn, VIn, KOut, VOut> usingSharedIntermediateCache(String cacheName, String cacheConfigurationName) {
      if (cacheConfigurationName == null || cacheConfigurationName.isEmpty()) {
         throw new IllegalArgumentException("Invalid configuration name " + cacheConfigurationName
               + ", cacheConfigurationName cannot be null or empty");
      }
      if (cacheName == null || cacheName.isEmpty()) {
         throw new IllegalArgumentException("Invalid cache name" + cacheName + ", cache name cannot be null or empty");
      }
      this.customIntermediateCacheName = cacheName;
      this.intermediateCacheConfigurationName = cacheConfigurationName;
      this.useIntermediateSharedCache = true;
      return this;
   }

   /**
    * Executes this task across Infinispan cluster nodes.
    *
    * @return a Map where each key is an output key and value is reduced value for that output key
    */
   public Map<KOut, VOut> execute() throws CacheException {
      return executeHelper(null);
   }

   /**
    * Executes this task and stores results in the provided results cache. The results can be
    * obtained once the execute method completes i.e., execute method is synchronous.
    *
    * This variant of execute method minimizes the possibility of the master JVM node exceeding
    * its allowed maximum heap, especially if objects that are results of the reduce phase have a
    * large memory footprint and/or multiple MapReduceTasks are executed concurrently on the master
    * task node.
    *
    * @param resultsCache
    *           application provided results cache
    * @throws CacheException
    *
    * @since 7.0
    */
   public void execute(Cache<KOut, VOut> resultsCache) throws CacheException {
      executeHelper(resultsCache.getName());
   }

   /**
    * Executes this task and stores results in the provided results cache. The results can be
    * obtained once the execute method completes i.e., execute method is synchronous.
    *
    * This variant of execute method minimizes the possibility of the master JVM node exceeding its
    * allowed maximum heap, especially if objects that are results of the reduce phase have a large
    * memory footprint and/or multiple MapReduceTasks are executed concurrently on the master task
    * node.
    *
    * @param resultsCache
    *           application provided results cache represented by its name
    * @throws CacheException
    *
    * @since 7.0
    */
   public void execute(String resultsCache) throws CacheException {
      if (resultsCache == null || resultsCache.isEmpty()) {
         throw new IllegalArgumentException("Results cache can not be " + resultsCache);
      }
      executeHelper(resultsCache);
   }

   protected Map<KOut, VOut> executeHelper(String resultCache) throws NullPointerException, MapReduceException {
      ensureAccessPermissions(cache);
      if (mapper == null)
         throw new NullPointerException("A valid reference of Mapper is not set " + mapper);

      if (reducer == null)
         throw new NullPointerException("A valid reference of Reducer is not set " + reducer);

      Map<KOut,VOut> result = null;
      if(!isLocalOnly && distributeReducePhase()){
         // init and create tmp caches
         executeTaskInit(getIntermediateCacheName());
         Set<KOut> allMapPhasesResponses = null;
         try {
            // map
            allMapPhasesResponses = executeMapPhase();

            // reduce
            result = executeReducePhase(resultCache, allMapPhasesResponses, useIntermediateSharedCache());
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MapReduceException(ie);
         }
         finally {
            // cleanup tmp caches across cluster
            EmbeddedCacheManager cm = cache.getCacheManager();
            String intermediateCache = getIntermediateCacheName();
            if (useIntermediatePerTaskCache()) {
               cm.removeCache(intermediateCache);
            } else {
               //let's make sure shared cache is not destroyed and that we have keys to remove
               Cache<KOut, VOut> sharedTmpCache = cm.getCache(intermediateCache);
               if (sharedTmpCache != null && allMapPhasesResponses != null) {
                  for (KOut k : allMapPhasesResponses) {
                     sharedTmpCache.removeAsync(new IntermediateKey<KOut>(taskId.toString(), k));
                  }
               }
            }
         }
      } else {
         try {
            if (resultCache == null || resultCache.isEmpty()) {
               result = new HashMap<KOut, VOut>();
               executeMapPhaseWithLocalReduction(result);
            } else {
               EmbeddedCacheManager cm = cache.getCacheManager();
               Cache<KOut, VOut> c = cm.getCache(resultCache);
               executeMapPhaseWithLocalReduction(c);
            }
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MapReduceException(ie);
         }
      }
      return result;
   }

   protected String getIntermediateCacheName() {
      return customIntermediateCacheName;
   }

   protected boolean distributeReducePhase(){
      return distributeReducePhase;
   }

   protected boolean useIntermediateSharedCache() {
      return useIntermediateSharedCache;
   }

   protected boolean useIntermediatePerTaskCache() {
      return !useIntermediateSharedCache();
   }


   protected void executeTaskInit(String tmpCacheName) throws MapReduceException {
      RpcManager rpc = cache.getRpcManager();
      CommandsFactory factory = cache.getComponentRegistry().getComponent(CommandsFactory.class);

      //first create tmp caches on all nodes
      final CreateCacheCommand ccc = factory.buildCreateCacheCommand(tmpCacheName, intermediateCacheConfigurationName, true, rpc.getMembers().size());
      log.debugf("Invoking %s across members %s ", ccc, cache.getRpcManager().getMembers());

      // invoke remotely
      CompletableFuture<Map<Address, Response>> remoteFuture = rpc.invokeRemotelyAsync(
            cache.getRpcManager().getMembers(), ccc, rpcOptionsBuilder.build());

      // invoke locally
      try {
         ccc.init(cache.getCacheManager(), stateTransferManager);
         try {
            ccc.perform(null);
         } catch (Throwable e) {
            throw new MapReduceException("Could not initialize temporary caches for MapReduce task on remote nodes ", e);
         }
      } catch (Exception e) {
         throw new MapReduceException(e);
      }

      // process remote responses
      try {
         Map<Address, Response> map = remoteFuture.get();
         for (Entry<Address, Response> e : map.entrySet()) {
            if (!e.getValue().isSuccessful()) {
               throw new MapReduceException(
                     "Could not initialize tmp cache " + tmpCacheName + " at " + e.getKey() + " for  " +
                     this);
            }
         }
      } catch (InterruptedException | ExecutionException e) {
         throw new MapReduceException(
               "Could not initialize temporary caches for MapReduce task on remote nodes ", e);
      }
   }

   protected Set<KOut> executeMapPhase() throws MapReduceException, InterruptedException {
      RpcManager rpc = cache.getRpcManager();
      MapCombineCommand<KIn, VIn, KOut, VOut> cmd = null;
      Set<KOut> mapPhasesResult = new HashSet<KOut>();
      List<MapTaskPart<Set<KOut>>> futures = new ArrayList<MapTaskPart<Set<KOut>>>();
      if (inputTaskKeysEmpty()) {
         for (Address target : rpc.getMembers()) {
            if (target.equals(rpc.getAddress())) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                     getIntermediateCacheName(), null, true, useIntermediateSharedCache());
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, getIntermediateCacheName(), null,
                     true, useIntermediateSharedCache());
            }
            MapTaskPart<Set<KOut>> part = createTaskMapPart(cmd, target, true);
            part.execute();
            futures.add(part);
         }
      } else {
         Map<Address, ? extends Collection<KIn>> keysToNodes = mapKeysToNodes(keys);
         for (Entry<Address, ? extends Collection<KIn>> e : keysToNodes.entrySet()) {
            Address address = e.getKey();
            Collection<KIn> keys = e.getValue();
            if (address.equals(rpc.getAddress())) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                     getIntermediateCacheName(), keys, true, useIntermediateSharedCache());
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, getIntermediateCacheName(), keys,
                     true, useIntermediateSharedCache());
            }
            MapTaskPart<Set<KOut>> part = createTaskMapPart(cmd, address, true);
            part.execute();
            futures.add(part);
         }
      }
      try {
         for (MapTaskPart<Set<KOut>> mapTaskPart : futures) {
            Set<KOut> result = null;
            try {
               result = mapTaskPart.get();
            } catch (ExecutionException ee) {
               throw new MapReduceException("Map phase failed ", ee.getCause());
            }
            mapPhasesResult.addAll(result);
         }
      } finally {
         cancellableTasks.clear();
      }
      return mapPhasesResult;
   }

   protected void executeMapPhaseWithLocalReduction(Map<KOut, VOut> reducedResult) throws MapReduceException, InterruptedException {
      RpcManager rpc = SecurityActions.getCacheRpcManager(cache);
      MapCombineCommand<KIn, VIn, KOut, VOut> cmd = null;
      Map<KOut, List<VOut>> mapPhasesResult = new HashMap<KOut, List<VOut>>();
      List<MapTaskPart<Map<KOut, List<VOut>>>> futures = new ArrayList<MapTaskPart<Map<KOut, List<VOut>>>>();
      Address localAddress = clusteringDependentLogic.getAddress();
      if (inputTaskKeysEmpty()) {
         List<Address> targets;
         if (isLocalOnly) {
            targets = Collections.singletonList(localAddress);
         } else {
            targets = rpc.getMembers();
         }
         for (Address target : targets) {
            if (target.equals(localAddress)) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                     getIntermediateCacheName(), null, false, false);
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, getIntermediateCacheName(), null,
                     false, false);
            }
            MapTaskPart<Map<KOut, List<VOut>>> part = createTaskMapPart(cmd, target, false);
            part.execute();
            futures.add(part);
         }
      } else {
         Map<Address, ? extends Collection<KIn>> keysToNodes = mapKeysToNodes(keys);
         for (Entry<Address, ? extends Collection<KIn>> e : keysToNodes.entrySet()) {
            Address address = e.getKey();
            Collection<KIn> keys = e.getValue();
            if (address.equals(localAddress)) {
               cmd = buildMapCombineCommand(taskId.toString(), clone(mapper), clone(combiner),
                     getIntermediateCacheName(), keys, false, false);
            } else {
               cmd = buildMapCombineCommand(taskId.toString(), mapper, combiner, getIntermediateCacheName(), keys,
                     false, false);
            }
            MapTaskPart<Map<KOut, List<VOut>>> part = createTaskMapPart(cmd, address, false);
            part.execute();
            futures.add(part);
         }
      }
      try {
         for (MapTaskPart<Map<KOut, List<VOut>>> mapTaskPart : futures) {
            Map<KOut, List<VOut>> result = null;
            try {
               result = mapTaskPart.get();
            } catch (ExecutionException ee) {
               throw new MapReduceException("Map phase failed ", ee.getCause());
            }
            mergeResponse(mapPhasesResult, result);
         }
      } finally {
         cancellableTasks.clear();
      }

      // hook into lifecycle
      MapReduceTaskLifecycleService taskLifecycleService = MapReduceTaskLifecycleService
               .getInstance();
      log.tracef("For m/r task %s invoking %s locally", taskId, reducer);
      try {
         taskLifecycleService.onPreExecute(reducer, cache);
         for (Entry<KOut, List<VOut>> e : mapPhasesResult.entrySet()) {
            // TODO in parallel with futures
            reducedResult.put(e.getKey(), reducer.reduce(e.getKey(), e.getValue().iterator()));
         }
      } finally {
         taskLifecycleService.onPostExecute(reducer);
      }
   }

   protected <V> MapTaskPart<V> createTaskMapPart(MapCombineCommand<KIn, VIn, KOut, VOut> cmd,
            Address target, boolean distributedReduce) {
      MapTaskPart<V> mapTaskPart = new MapTaskPart<V>(target, cmd, distributedReduce);
      cancellableTasks.add(mapTaskPart);
      return mapTaskPart;
   }

   protected Map<KOut, VOut> executeReducePhase(String resultCache, Set<KOut> allMapPhasesResponses,
            boolean useIntermediateSharedCache) throws MapReduceException, InterruptedException {
      RpcManager rpc = cache.getRpcManager();
      String destCache = getIntermediateCacheName();

      Cache<Object, Object> dstCache = cache.getCacheManager().getCache(destCache);
      Map<Address, ? extends Collection<KOut>> keysToNodes = mapKeysToNodes(dstCache.getAdvancedCache()
               .getDistributionManager(), allMapPhasesResponses, useIntermediateSharedCache);
      Map<KOut, VOut> reduceResult = new HashMap<KOut, VOut>();
      List<ReduceTaskPart<Map<KOut, VOut>>> reduceTasks = new ArrayList<ReduceTaskPart<Map<KOut, VOut>>>();
      ReduceCommand<KOut, VOut> reduceCommand = null;
      for (Entry<Address, ? extends Collection<KOut>> e : keysToNodes.entrySet()) {
         Address address = e.getKey();
         Collection<KOut> keys = e.getValue();
         if (address.equals(rpc.getAddress())) {
            reduceCommand = buildReduceCommand(resultCache, taskId.toString(), destCache, clone(reducer), keys,
                     useIntermediateSharedCache);
         } else {
            reduceCommand = buildReduceCommand(resultCache, taskId.toString(), destCache, reducer, keys,
                     useIntermediateSharedCache);
         }
         ReduceTaskPart<Map<KOut, VOut>> part = createReducePart(reduceCommand, address, destCache);
         part.execute();
         reduceTasks.add(part);
      }
      try {
         for (ReduceTaskPart<Map<KOut, VOut>> reduceTaskPart : reduceTasks) {
            Map<KOut, VOut> result = null;
            try {
               result = reduceTaskPart.get();
            } catch (ExecutionException ee) {
               throw new MapReduceException("Reduce phase failed", ee.getCause());
            }
            reduceResult.putAll(result);
         }
      } finally {
         cancellableTasks.clear();
      }
      return reduceResult;
   }

   protected <V> ReduceTaskPart<V> createReducePart(ReduceCommand<KOut, VOut> cmd, Address target,
            String destCacheName) {
      ReduceTaskPart<V> part = new ReduceTaskPart<V>(target, cmd, destCacheName);
      cancellableTasks.add(part);
      return part;
   }

   private <K, V> void mergeResponse(Map<K, List<V>> result, Map<K, List<V>> m) {
      for (Entry<K, List<V>> entry : m.entrySet()) {
         synchronized (result) {
            List<V> list = result.get(entry.getKey());
            if (list != null) {
               list.addAll(entry.getValue());
            } else {
               list = new ArrayList<V>();
               list.addAll(entry.getValue());
            }
            result.put(entry.getKey(), list);
         }
      }
   }

   private MapCombineCommand<KIn, VIn, KOut, VOut> buildMapCombineCommand(
            String taskId, Mapper<KIn, VIn, KOut, VOut> m, Reducer<KOut, VOut> r, String intermediateCacheName,
            Collection<KIn> keys, boolean reducePhaseDistributed, boolean useIntermediateSharedCache){
      ComponentRegistry registry = SecurityActions.getCacheComponentRegistry(cache);
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      MapCombineCommand<KIn, VIn, KOut, VOut> c = factory.buildMapCombineCommand(taskId, m, r, keys);
      c.setReducePhaseDistributed(reducePhaseDistributed);
      c.setUseIntermediateSharedCache(useIntermediateSharedCache);
      c.setIntermediateCacheName(intermediateCacheName);
      c.setMaxCollectorSize(MAX_COLLECTOR_SIZE);
      return c;
   }

   private ReduceCommand<KOut, VOut> buildReduceCommand(String resultCacheName, String taskId, String destinationCache,
         Reducer<KOut, VOut> r, Collection<KOut> keys, boolean useIntermediateSharedCache) {
      ComponentRegistry registry = cache.getComponentRegistry();
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      ReduceCommand<KOut, VOut> reduceCommand = factory.buildReduceCommand(taskId, destinationCache, r, keys);
      reduceCommand.setUseIntermediateSharedCache(useIntermediateSharedCache);
      reduceCommand.setResultCacheName(resultCacheName);
      return reduceCommand;
   }

   private CancelCommand buildCancelCommand(CancellableTaskPart taskPart){
      ComponentRegistry registry = cache.getComponentRegistry();
      CommandsFactory factory = registry.getComponent(CommandsFactory.class);
      return factory.buildCancelCommandCommand(taskPart.getUUID());
   }

   /**
    * Executes this task across Infinispan cluster nodes asynchronously.
    *
    * @return a Future wrapping a Map where each key is an output key and value is reduced value for
    *         that output key
    */
   public Future<Map<KOut, VOut>> executeAsynchronously() {
      final MapReduceTaskFuture<Map<KOut, VOut>> result = new MapReduceTaskFuture<Map<KOut, VOut>>();
      ExecutorService executor = mapReduceManager.getExecutorService();
      Future<Map<KOut, VOut>> returnValue = executor.submit(new Callable<Map<KOut, VOut>>() {
         @Override
         public Map<KOut, VOut> call() throws Exception {
            try {
               Map<KOut, VOut> retval = execute();
               try {
                  result.notifyDone(retval);
                  log.trace("Finished notifying");
               } catch (Throwable e) {
                  log.trace("Exception while notifying the future", e);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
                  log.trace("Finished notifying exception");
               } catch (Throwable e2) {
                  log.trace("Exception while notifying the future", e2);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
      return result;
   }

   /**
    * Executes this task across Infinispan cluster but the final result is collated using specified
    * {@link Collator}
    *
    * @param collator
    *           a Collator to use
    *
    * @return collated result
    */
   public <R> R execute(Collator<KOut, VOut, R> collator) {
      Map<KOut, VOut> execute = execute();
      return collator.collate(execute);
   }

   /**
    * Executes this task asynchronously across Infinispan cluster; final result is collated using
    * specified {@link Collator} and wrapped by Future
    *
    * @param collator
    *           a Collator to use
    *
    * @return collated result
    */
   public <R> Future<R> executeAsynchronously(final Collator<KOut, VOut, R> collator) {
      final MapReduceTaskFuture<R> result = new MapReduceTaskFuture<R>();
      ExecutorService executor = mapReduceManager.getExecutorService();
      Future<R> returnValue = executor.submit(new Callable<R>() {
         @Override
         public R call() throws Exception {
            try {
               R retval = execute(collator);
               try {
                  result.notifyDone(retval);
                  log.trace("Finished notifying");
               } catch (Throwable e) {
                  log.trace("Exception while notifying the future", e);
               }
               return retval;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
                  log.trace("Finished notifying");
               } catch (Throwable e2) {
                  log.trace("Exception while notifying the future", e2);
               }
               throw e;
            }
         }
      });
      result.setFuture(returnValue);
      return result;
   }

   protected <T> Map<Address, ? extends Collection<T>> mapKeysToNodes(DistributionManager dm, Collection<T> keysToMap, boolean useIntermediateCompositeKey) {
      if (isLocalOnly) {
         return Collections.singletonMap(clusteringDependentLogic.getAddress(), keysToMap);
      } else {
         return mapReduceManager.mapKeysToNodes(dm, taskId.toString(), keysToMap);
      }
   }

   protected <T> Map<Address, ? extends Collection<T>> mapKeysToNodes(Collection<T> keysToMap, boolean useIntermediateCompositeKey) {
      return mapKeysToNodes(cache.getDistributionManager(), keysToMap, useIntermediateCompositeKey);
   }

   protected <T> Map<Address, ? extends Collection<T>> mapKeysToNodes(Collection<T> keysToMap) {
      return mapKeysToNodes(keysToMap, false);
   }

   protected Mapper<KIn, VIn, KOut, VOut> clone(Mapper<KIn, VIn, KOut, VOut> mapper){
      return Util.cloneWithMarshaller(marshaller, mapper);
   }

   protected Reducer<KOut, VOut> clone(Reducer<KOut, VOut> reducer){
      return Util.cloneWithMarshaller(marshaller, reducer);
   }

   private void ensureAccessPermissions(final AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.EXEC);
      }
   }

   private void ensureProperCacheState(AdvancedCache<KIn, VIn> cache) throws NullPointerException,
            IllegalStateException {
      if (cache.getStatus() != ComponentStatus.RUNNING)
         throw log.invalidCacheState(cache.getStatus().toString());

      if (SecurityActions.getCacheRpcManager(cache) != null && SecurityActions.getCacheDistributionManager(cache) == null) {
         throw log.requireDistOrReplCache(cache.getCacheConfiguration().clustering().cacheModeString());
      }
   }

   protected boolean inputTaskKeysEmpty() {
      return keys == null || keys.isEmpty();
   }
   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((taskId == null) ? 0 : taskId.hashCode());
      return result;
   }

   @SuppressWarnings("rawtypes")
   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof MapReduceTask)) {
         return false;
      }
      MapReduceTask other = (MapReduceTask) obj;
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
      return "MapReduceTask [mapper=" + mapper + ", reducer=" + reducer + ", combiner=" + combiner
               + ", keys=" + keys + ", taskId=" + taskId + "]";
   }

   private class MapReduceTaskFuture<R> extends NotifyingFutureImpl<R> {

      private volatile boolean cancelled = false;

      public MapReduceTaskFuture() {
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         if (!isCancelled()) {
            RpcManager rpc = cache.getRpcManager();
            synchronized (cancellableTasks) {
               for (CancellableTaskPart task : cancellableTasks) {
                  boolean sendingToSelf = task.getExecutionTarget().equals(
                           rpc.getTransport().getAddress());
                  CancelCommand cc = buildCancelCommand(task);
                  if (sendingToSelf) {
                     cc.init(cancellationService);
                     try {
                        cc.perform(null);
                     } catch (Throwable e) {
                        log.couldNotExecuteCancellationLocally(e.getLocalizedMessage());
                     }
                  } else {
                     rpc.invokeRemotely(Collections.singletonList(task.getExecutionTarget()), cc, rpcOptionsBuilder.build());
                  }
                  cancelled = true;
               }
            }
            return cancelled;
         } else {
            //already cancelled
            return false;
         }
      }

      @Override
      public boolean isCancelled() {
         return cancelled;
      }
   }

   private abstract class TaskPart<V> implements Future<V>, CancellableTaskPart {

      private Future<Map<Address, Response>> f;
      private final Address executionTarget;

      public TaskPart(Address executionTarget) {
         this.executionTarget = executionTarget;
      }

      @Override
      public Address getExecutionTarget() {
         return executionTarget;
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
         return false;
      }

      @Override
      public boolean isCancelled() {
         return false;
      }

      @Override
      public boolean isDone() {
         return false;
      }

      @Override
      public V get() throws InterruptedException, ExecutionException {
         return retrieveResult(f.get());
      }

      protected Address getAddress() {
         return clusteringDependentLogic.getAddress();
      }

      protected boolean locallyExecuted(){
         return getAddress().equals(getExecutionTarget());
      }

      public abstract void execute();

      @SuppressWarnings("unchecked")
      private V retrieveResult(Object response) throws ExecutionException {
         if (response == null) {
            throw new ExecutionException("Execution returned null value",
                     new NullPointerException());
         }
         if (response instanceof Exception) {
            throw new ExecutionException((Exception) response);
         }

         Map<Address, Response> mapResult = (Map<Address, Response>) response;
         assert mapResult.size() == 1;
         for (Entry<Address, Response> e : mapResult.entrySet()) {
            if (e.getValue() instanceof SuccessfulResponse) {
               return (V) ((SuccessfulResponse) e.getValue()).getResponseValue();
            }
         }
         throw new ExecutionException(new IllegalStateException("Invalid response " + response));
      }

      @Override
      public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
               TimeoutException {
         return retrieveResult(f.get(timeout, unit));
      }

      public void setFuture(Future<Map<Address, Response>> future) {
         this.f = future;
      }
   }

   private class MapTaskPart<V> extends TaskPart<V> {

      private final MapCombineCommand<KIn, VIn, KOut, VOut> mcc;
      private final boolean distributedReduce;

      public MapTaskPart(Address executionTarget, MapCombineCommand<KIn, VIn, KOut, VOut> command,
               boolean distributedReduce) {
         super(executionTarget);
         this.mcc = command;
         this.distributedReduce = distributedReduce;
      }

      @Override
      @SuppressWarnings("unchecked")
      public void execute() {
         if (locallyExecuted()) {
            Callable<Map<Address, Response>> callable;
            if (distributedReduce) {
               callable = () -> {
                  Set<KOut> result = invokeMapCombineLocally();
                  return Collections.singletonMap(getAddress(), SuccessfulResponse.create(result));
               };
            } else {
               callable = () -> {
                  Map<KOut, List<VOut>> result = invokeMapCombineLocallyForLocalReduction();
                  return Collections.singletonMap(getAddress(), SuccessfulResponse.create(result));
               };
            }
            Future<Map<Address, Response>> localFuture = mapReduceManager.getExecutorService().submit(
                  callable);
            setFuture(localFuture);
         } else {
            RpcManager rpc = SecurityActions.getCacheRpcManager(cache);
            try {
               log.debugf("Invoking %s on %s", mcc, getExecutionTarget());
               CompletableFuture<Map<Address, Response>> remoteFuture = rpc.invokeRemotelyAsync(
                     Collections.singleton(getExecutionTarget()), mcc, rpcOptionsBuilder.build());
               setFuture(remoteFuture);
               log.debugf("Invoked %s on %s ", mcc, getExecutionTarget());
            } catch (Exception ex) {
               throw new MapReduceException(
                        "Could not invoke map phase of MapReduceTask on remote node "
                                 + getExecutionTarget(), ex);
            }
         }
      }

      private Map<KOut, List<VOut>> invokeMapCombineLocallyForLocalReduction() throws InterruptedException {
         log.debugf("Invoking %s locally", mcc);
         try {
            cancellationService.register(Thread.currentThread(), mcc.getUUID());
            mcc.init(mapReduceManager);
            return mapReduceManager.mapAndCombineForLocalReduction(mcc);
         } finally {
            cancellationService.unregister(mcc.getUUID());
            log.debugf("Invoked %s locally", mcc);
         }
      }

      private Set<KOut> invokeMapCombineLocally() throws InterruptedException {
         log.debugf("Invoking %s locally", mcc);
         try {
            cancellationService.register(Thread.currentThread(), mcc.getUUID());
            mcc.init(mapReduceManager);
            return mapReduceManager.mapAndCombineForDistributedReduction(mcc);
         } finally {
            cancellationService.unregister(mcc.getUUID());
            log.debugf("Invoked %s locally", mcc);
         }
      }

      @Override
      public UUID getUUID() {
         return mcc.getUUID();
      }
   }

   private class ReduceTaskPart<V> extends TaskPart<V> {

      private final ReduceCommand<KOut, VOut> rc;
      private final String cacheName;

      public ReduceTaskPart(Address executionTarget, ReduceCommand<KOut, VOut> command,
               String destinationCacheName) {
         super(executionTarget);
         this.rc = command;
         this.cacheName = destinationCacheName;
      }

      @Override
      @SuppressWarnings("unchecked")
      public void execute() {
         if (locallyExecuted()) {
            Callable<Map<Address, Response>> callable = () -> {
               Cache<Object, Object> dstCache = cache.getCacheManager().getCache(cacheName);
               Map<KOut, VOut> result = invokeReduceLocally(dstCache);
               return Collections.singletonMap(getAddress(), SuccessfulResponse.create(result));
            };
            Future<Map<Address, Response>> future = mapReduceManager.getExecutorService().submit(callable);
            setFuture(future);
         } else {
            RpcManager rpc = cache.getRpcManager();
            try {
               log.debugf("Invoking %s on %s", rc, getExecutionTarget());
               CompletableFuture<Map<Address, Response>> future = rpc.invokeRemotelyAsync(
                     Collections.singleton(getExecutionTarget()), rc, rpcOptionsBuilder.build());
               setFuture(future);
               log.debugf("Invoked %s on %s ", rc, getExecutionTarget());
            } catch (Exception ex) {
               throw new MapReduceException(
                        "Could not invoke map phase of MapReduceTask on remote node "
                                 + getExecutionTarget(), ex);
            }
         }
      }

      private Map<KOut, VOut> invokeReduceLocally(Cache<Object, Object> dstCache) {
         rc.init(mapReduceManager);
         Map<KOut, VOut> localReduceResult = null;
         try {
            log.debugf("Invoking %s locally ", rc);
            if(rc.emitsIntoResultingCache()){
               mapReduceManager.reduce(rc, rc.getResultCacheName());
               localReduceResult = Collections.emptyMap();
            } else {
               localReduceResult = mapReduceManager.reduce(rc);
            }
            log.debugf("Invoked %s locally", rc);
         } catch (Throwable e1) {
            throw new MapReduceException("Could not invoke MapReduce task locally ", e1);
         }
         return localReduceResult;
      }

      @Override
      public UUID getUUID() {
         return rc.getUUID();
      }
   }

   private interface CancellableTaskPart {
      UUID getUUID();
      Address getExecutionTarget();
   }
}
