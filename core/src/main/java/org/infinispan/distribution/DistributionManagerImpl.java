package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.infinispan.context.Flag.*;
import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
public class DistributionManagerImpl implements DistributionManager {
   private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Configuration configuration;
   private volatile ConsistentHash consistentHash;
   private Address self;
   private CacheLoaderManager cacheLoaderManager;
   private RpcManager rpcManager;
   private CacheManagerNotifier notifier;

   private ViewChangeListener listener;
   private CommandsFactory cf;

   private final ExecutorService rehashExecutor;

   private TransactionLogger transactionLogger;

   private TopologyInfo topologyInfo = new TopologyInfo();

   /**
    * Rehash flag set by a rehash task associated with this DistributionManager
    */
   volatile boolean rehashInProgress = false;

   /**
    * https://issues.jboss.org/browse/ISPN-925 This makes sure that leavers list and consistent hash is updated
    * atomically.
    */
   private final ReentrantReadWriteLock chSwitchLock = new ReentrantReadWriteLock(true);
   

   private DataContainer dataContainer;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;

   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.")
   @Metric(displayName = "Is join completed?", dataType = DataType.TRAIT)
   private volatile boolean joinComplete = false;

   private Future<Void> joinFuture;
   private final ReclosableLatch startLatch = new ReclosableLatch(false);

   final CountDownLatch finalJoinPhaseLatch = new CountDownLatch(1);
   volatile boolean enteredFinalJoinPhase = false;
   InboundInvocationHandler inboundInvocationHandler;

   /**
    * Default constructor
    */
   public DistributionManagerImpl() {
      super();
      LinkedBlockingQueue<Runnable> rehashQueue = new LinkedBlockingQueue<Runnable>();
      ThreadFactory tf = new ThreadFactory() {
         public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setPriority(Thread.MIN_PRIORITY);
            t.setName("Rehasher-" + rpcManager.getTransport().getAddress());
            return t;
         }
      };
      rehashExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, rehashQueue, tf);
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CacheManagerNotifier notifier, CommandsFactory cf,
                    DataContainer dataContainer, InterceptorChain interceptorChain, InvocationContextContainer icc,
                    CacheLoaderManager cacheLoaderManager, InboundInvocationHandler inboundInvocationHandler) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.notifier = notifier;
      this.cf = cf;
      this.transactionLogger = new TransactionLoggerImpl(cf);
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.inboundInvocationHandler = inboundInvocationHandler;
   }

   // needs to be AFTER the RpcManager

   @Start(priority = 20)
   public void start() throws Exception {
      if (trace) log.trace("starting distribution manager on " + getMyAddress());
      listener = new ViewChangeListener();
      notifier.addListener(listener);
      GlobalConfiguration gc = configuration.getGlobalConfiguration();
      if (gc.hasTopologyInfo()) {
         Address address = rpcManager.getTransport().getAddress();
         NodeTopologyInfo nti = new NodeTopologyInfo(gc.getMachineId(), gc.getRackId(), gc.getSiteId(), address);
         topologyInfo.addNodeTopologyInfo(address, nti);
      }
      join();
   }

   private int getReplCount() {
      return configuration.getNumOwners();
   }

   private Address getMyAddress() {
      return rpcManager != null ? rpcManager.getAddress() : null;
   }

   public RpcManager getRpcManager() {
      return rpcManager;
   }

   // To avoid blocking other components' start process, wait last, if necessary, for join to complete.

   @Start(priority = 1000)
   public void waitForJoinToComplete() throws Throwable {
      if (joinFuture != null) {
         try {
            joinFuture.get();
         } catch (InterruptedException e) {
            throw e;
         } catch (ExecutionException e) {
            if (e.getCause() != null) throw e.getCause();
            else throw e;
         }
      }
   }

   private void join() throws Exception {
      startLatch.close();
      setJoinComplete(false);
      Transport t = rpcManager.getTransport();
      List<Address> members = t.getMembers();
      consistentHash = createConsistentHash(configuration, members, topologyInfo);
      self = t.getAddress();
      setJoinComplete(true);
      startLatch.open();
   }

   @Stop(priority = 20)
   public void stop() {
      notifier.removeListener(listener);
      rehashExecutor.shutdownNow();
      setJoinComplete(false);
   }


   public boolean isLocal(Object key) {
      return getLocality(key).isLocal();
   }

   public DataLocality getLocality(Object key) {
      chSwitchLock.readLock().lock();
      try {
         if (consistentHash == null) return DataLocality.LOCAL;

         boolean local = consistentHash.isKeyLocalToAddress(self, key, getReplCount());
         if (isRehashInProgress()) {
            if (local) {
               return DataLocality.LOCAL_UNCERTAIN;
            } else {
               return DataLocality.NOT_LOCAL_UNCERTAIN;
            }
         } else {
            if (local) {
               return DataLocality.LOCAL;
            } else {
               return DataLocality.NOT_LOCAL;
            }
         }

      } finally {
         chSwitchLock.readLock().unlock();
      }
   }


   public List<Address> locate(Object key) {
      if (consistentHash == null) return Collections.singletonList(self);
      return consistentHash.locate(key, getReplCount());
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys) {
      return locateAll(keys, getReplCount());
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys, int numOwners) {
      if (consistentHash == null) {
         Map<Object, List<Address>> m = new HashMap<Object, List<Address>>(keys.size());
         List<Address> selfList = Collections.singletonList(self);
         for (Object k : keys) m.put(k, selfList);
         return m;
      }
      return consistentHash.locateAll(keys, numOwners);
   }

   public void transformForL1(CacheEntry entry) {
      if (entry.getLifespan() < 0 || entry.getLifespan() > configuration.getL1Lifespan())
         entry.setLifespan(configuration.getL1Lifespan());
   }

   public InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx) throws Exception {
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key, ctx.getFlags());

      ResponseFilter filter = new ClusteredGetResponseValidityFilter(locate(key));
      Map<Address, Response> responses = rpcManager.invokeRemotely(locate(key), get, ResponseMode.SYNCHRONOUS,
                                                                   configuration.getSyncReplTimeout(), false, filter);

      if (!responses.isEmpty()) {
         for (Response r : responses.values()) {
            if (r instanceof SuccessfulResponse) {
               InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
               return cacheValue.toInternalCacheEntry(key);
            }
         }
      }

      return null;
   }

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }

   public void setConsistentHash(ConsistentHash consistentHash) {
      if (trace) log.trace("installing new consistent hash %s", consistentHash);
      this.consistentHash = consistentHash;
   }


   @ManagedOperation(description = "Determines whether a given key is affected by an ongoing rehash, if any.")
   @Operation(displayName = "Could key be affected by rehash?")
   public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
      return transactionLogger.isEnabled() && consistentHash != null && !consistentHash.locate(key, getReplCount()).contains(self);
   }

   public TransactionLogger getTransactionLogger() {
      return transactionLogger;
   }



   private Map<Object, InternalCacheValue> applyStateMap(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state, boolean withRetry) {
      Map<Object, InternalCacheValue> retry = withRetry ? new HashMap<Object, InternalCacheValue>() : null;
      Address myself=self;
      if(myself == null) {
         myself=rpcManager.getTransport().getAddress();
         self=myself;
      }

      for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
         if (consistentHash.locate(e.getKey(), configuration.getNumOwners()).contains(myself)) {
            InternalCacheValue v = e.getValue();
            InvocationContext ctx = icc.createInvocationContext();
            ctx.setFlags(CACHE_MODE_LOCAL, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING, FORCE_COMMIT); // locking not necessary in the case of a join since the node isn't doing anything else.

            try {
               PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle(), ctx.getFlags());
               interceptorChain.invoke(ctx, put);
            } catch (Exception ee) {
               if (withRetry) {
                  if (trace)
                     log.trace("problem %s encountered when applying state for key %s. Adding entry to retry queue.", ee.getMessage(), e.getKey());
                  retry.put(e.getKey(), e.getValue());
               } else {
                  log.warn("problem %s encountered when applying state for key %s!", ee.getMessage(), e.getKey());
               }
            }
         }
      }
      return retry;
   }

   // todo: forLeave is always set to true in RehashControlCommand, check if this is correct
   public void applyState(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state,
                          RemoteTransactionLogger tlog, boolean forLeave, Address sender) {
      if (trace) log.trace("received %d keys from %s", state.size(), sender);
      int retryCount = 3; // in case we have issues applying state.
      Map<Object, InternalCacheValue> pendingApplications = state;
      for (int i = 0; i < retryCount; i++) {
         pendingApplications = applyStateMap(consistentHash, pendingApplications, true);
         if (pendingApplications.isEmpty()) break;
      }
      // one last go
      if (!pendingApplications.isEmpty()) applyStateMap(consistentHash, pendingApplications, false);

      if (!forLeave) drainLocalTransactionLog(tlog);

      if(trace)
         log.trace("data container has now %d keys", dataContainer.size());
   }

   public void setRehashInProgress(boolean value) {
      rehashInProgress = value;
   }

   @Listener
   public class ViewChangeListener {

      @Merged @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         if(trace)
            log.trace("view: type=" + e.getType() + ", members: " + e.getNewMembers() + ". Starting the RebalanceTask");
         RebalanceTask rebalanceTask = new RebalanceTask(rpcManager, cf, configuration, dataContainer,
                                                         DistributionManagerImpl.this, inboundInvocationHandler);
         joinFuture = rehashExecutor.submit(rebalanceTask);
      }
   }

   public CacheStore getCacheStoreForRehashing() {
      if (cacheLoaderManager == null || !cacheLoaderManager.isEnabled() || cacheLoaderManager.isShared())
         return null;
      return cacheLoaderManager.getCacheStore();
   }

   @ManagedAttribute(description = "Checks whether the node is involved in a rehash.")
   @Metric(displayName = "Is rehash in progress?", dataType = DataType.TRAIT)
   public boolean isRehashInProgress() {
      return rehashInProgress;
   }


   public boolean isJoinComplete() {
      return joinComplete;
   }

   public void waitForFinalJoin() {
      if (enteredFinalJoinPhase) {
         try {
            finalJoinPhaseLatch.await();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public boolean isInFinalJoinPhase() {
      return enteredFinalJoinPhase;
   }

   public void setJoinComplete(boolean joinComplete) {
      this.joinComplete = joinComplete;
      if (joinComplete) this.finalJoinPhaseLatch.countDown();
   }

   void drainLocalTransactionLog(RemoteTransactionLogger tlog) {
      List<WriteCommand> c;
      while (tlog.shouldDrainWithoutLock()) {
         c = tlog.drain();
         if (trace) log.trace("draining %s entries from transaction log", c.size());
         applyRemoteTxLog(c);
      }

      boolean unlock = acquireDistSyncLock();
      try {
         this.enteredFinalJoinPhase = true;
         c = tlog.drainAndLock(null);
         if (trace) log.trace("locked and draining %s entries from transaction log", c.size());
         applyRemoteTxLog(c);

         Collection<PrepareCommand> pendingPrepares = tlog.getPendingPrepares();
         if (trace) log.trace("applying %s pending prepares", pendingPrepares.size());
         for (PrepareCommand pc : pendingPrepares) {
            // this is a remotely originating call.
            cf.initializeReplicableCommand(pc, true);
            try {
               pc.perform(null);
            } catch (Throwable throwable) {
               log.warn("Unable to apply prepare " + pc, throwable);
            }
         }
      } finally {
         tlog.unlockAndDisable(null);
         if (unlock) rpcManager.getTransport().getDistributedSync().releaseProcessingLock(true);
      }
   }

   private boolean acquireDistSyncLock() {
      try {
         rpcManager.getTransport().getDistributedSync().acquireProcessingLock(true, 100, TimeUnit.DAYS);
         return true;
      } catch (TimeoutException e) {
         log.info("couldn't acquire shared lock");
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return false;
   }

   public List<Address> getAffectedNodes(Set<Object> affectedKeys) {
      if (affectedKeys == null || affectedKeys.isEmpty()) {
         if (log.isTraceEnabled()) log.trace("affected keys are empty");
         return Collections.emptyList();
      }

      Set<Address> an = new HashSet<Address>();
      for (List<Address> addresses : locateAll(affectedKeys).values()) an.addAll(addresses);
      return new ArrayList<Address>(an);
   }

   public void applyRemoteTxLog(List<WriteCommand> commands) {
      for (WriteCommand cmd : commands) {
         try {
            // this is a remotely originating tx
            cf.initializeReplicableCommand(cmd, true);
            InvocationContext ctx = icc.createInvocationContext();
            ctx.setFlags(SKIP_REMOTE_LOOKUP, CACHE_MODE_LOCAL, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING);
            interceptorChain.invoke(ctx, cmd);
         } catch (Exception e) {
            log.warn("caught exception replaying %s", e, cmd);
         }
      }

   }

   @ManagedOperation(description = "Tells you whether a given key is local to this instance of the cache.  Only works with String keys.")
   @Operation(displayName = "Is key local?")
   public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
      return getLocality(key).isLocal();
   }

   @ManagedOperation(description = "Locates an object in a cluster.  Only works with String keys.")
   @Operation(displayName = "Locate key")
   public List<String> locateKey(@Parameter(name = "key", description = "Key to locate") String key) {
      List<String> l = new LinkedList<String>();
      for (Address a : locate(key)) l.add(a.toString());
      return l;
   }

   @Override
   public String toString() {
      return "DistributionManagerImpl[rehashInProgress=" + rehashInProgress + ", consistentHash=" + consistentHash + "]";
   }

   public TopologyInfo getTopologyInfo() {
      return topologyInfo;
   }

  
}
