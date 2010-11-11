package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;

import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.distribution.ch.UnionConsistentHash;
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
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.MembershipArithmetic;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;
import org.rhq.helpers.pluginAnnotations.agent.Parameter;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
public class DistributionManagerImpl implements DistributionManager {
   private final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   
   private Configuration configuration;
   private volatile ConsistentHash consistentHash, oldConsistentHash;
   private Address self;
   private CacheLoaderManager cacheLoaderManager;
   RpcManager rpcManager;
   private CacheManagerNotifier notifier;

   private ViewChangeListener listener;
   private CommandsFactory cf;
   
   private final ExecutorService rehashExecutor;

   private final TransactionLogger transactionLogger = new TransactionLoggerImpl();

   TopologyInfo topologyInfo = new TopologyInfo();
   
   /**
    * Rehash flag set by a rehash task associated with this DistributionManager
    */
   volatile boolean rehashInProgress = false;
   
   
   /**
    * Address of a joiner node requesting to join Infinispan cluster. Each node in the cluster is
    * aware of joiner's identity. After joiner successfully joins (or fails to join), joiner field is nullified
    */
   private volatile Address joiner;
   
   private static final AtomicReferenceFieldUpdater<DistributionManagerImpl, Address> JOINER_CAS =
           AtomicReferenceFieldUpdater.newUpdater(DistributionManagerImpl.class, Address.class, "joiner");
   
   private DataContainer dataContainer;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   
   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.")
   @Metric(displayName = "Is join completed?", dataType = DataType.TRAIT)
   private volatile boolean joinComplete = false;
   
   private Future<Void> joinFuture;
   private final List<Address> leavers = new CopyOnWriteArrayList<Address>();
   private volatile Future<Void> leaveTaskFuture;
   private final ReclosableLatch startLatch = new ReclosableLatch(false);
   
   private Lock leaveAcks = new ReentrantLock();
   private Condition ackArrived = leaveAcks.newCondition();
   private List<Address> leaveRehashAcks = Collections.synchronizedList(new ArrayList<Address>());
  

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
                    CacheLoaderManager cacheLoaderManager) {
      this.cacheLoaderManager = cacheLoaderManager;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.notifier = notifier;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
   }

   // needs to be AFTER the RpcManager

   @Start(priority = 20)
   public void start() throws Exception {
      log.trace("Starting distribution manager on " + getMyAddress());            
      listener = new ViewChangeListener();
      notifier.addListener(listener);
      GlobalConfiguration gc = configuration.getGlobalConfiguration();
      if (gc.hasTopologyInfo()) {
         Address address = rpcManager.getTransport().getAddress();
         NodeTopologyInfo nti = new NodeTopologyInfo(gc.getMachineId(),gc.getRackId(), gc.getSiteId(), address);
         topologyInfo.addNodeTopologyInfo(address, nti);
      }
      join();
   }
   
   private int getReplCount(){
      return configuration.getNumOwners();
   }

   private Address getMyAddress() {
      return rpcManager != null ? rpcManager.getAddress() : null;
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
      if (members.size() > 1 && !t.getCoordinator().equals(self)) {
         JoinTask joinTask = new JoinTask(rpcManager, cf, configuration, dataContainer, this);
         joinFuture = rehashExecutor.submit(joinTask);
         //task will set joinComplete flag
      } else {
         setJoinComplete(true);
      }
      startLatch.open();
   }

   @Stop(priority = 20)
   public void stop() {
      notifier.removeListener(listener);
      rehashExecutor.shutdownNow();
      setJoinComplete(false);
   }

   public void rehash(List<Address> newMembers, List<Address> oldMembers) {
      boolean join = oldMembers.size() < newMembers.size();
      // on view change, we should update our view
      log.info("Detected a view change.  Member list changed from {0} to {1}", oldMembers, newMembers);

      if (join) {
         Address joiner = MembershipArithmetic.getMemberJoined(oldMembers, newMembers);
         log.info("This is a JOIN event!  Wait for notification from new joiner " + joiner);
      } else {
         Address leaver = MembershipArithmetic.getMemberLeft(oldMembers, newMembers);
         log.info("This is a LEAVE event!  Node {0} has just left", leaver);


         try {
            if (!(consistentHash instanceof UnionConsistentHash)) {
              oldConsistentHash = consistentHash;
            }  else {
               oldConsistentHash = ((UnionConsistentHash) consistentHash).getNewCH();
            }
            consistentHash = ConsistentHashHelper.removeAddress(consistentHash, leaver, configuration, topologyInfo);
         } catch (Exception e) {
            log.fatal("Unable to process leaver!!", e);
            throw new CacheException(e);
         }

         List<Address> stateProviders = holdersOfLeaversState(leaver);
         List<Address> receiversOfLeaverState = receiversOfLeaverState(stateProviders);
         boolean willReceiveLeaverState = receiversOfLeaverState.contains(self);
         boolean willProvideState = stateProviders.contains(self);    
         if (willReceiveLeaverState || willProvideState) {
            log.info("I {0} am participating in rehash, state providers {1}, state receivers {2}",
                     rpcManager.getTransport().getAddress(), stateProviders, receiversOfLeaverState);               
            transactionLogger.enable();

            if (leaveTaskFuture != null
                    && (!leaveTaskFuture.isCancelled() || !leaveTaskFuture.isDone())) {
               if (log.isTraceEnabled()) {
                  log.trace("Canceling running leave task!");
               }
               leaveTaskFuture.cancel(true);
            }

            leavers.add(leaver);
            InvertedLeaveTask task = new InvertedLeaveTask(this, rpcManager, configuration, cf,
                    dataContainer, leavers, stateProviders, receiversOfLeaverState, willReceiveLeaverState);
            leaveTaskFuture = rehashExecutor.submit(task);          
         } else {
            log.info("Not in same subspace, so ignoring leave event");
            topologyInfo.removeNodeInfo(leaver);
         }
      }
   }

   List<Address> holdersOfLeaversState(Address leaver) {
      List<Address> result = new ArrayList<Address>();
      for (Address addr : oldConsistentHash.getCaches()) {
         List<Address> backups = oldConsistentHash.getBackupsForNode(addr, getReplCount());
         log.trace("Backups for {0} are {1}", addr, backups);
         if (addr.equals(leaver)) {
            if (backups.size() > 1) {
               Address mainBackup = backups.get(1);
               result.add(mainBackup);
               log.trace("Leaver's ({0}) main backup({1}) is looking for another backup as well.", leaver, mainBackup);
            }
         } else if (backups.contains(leaver)) {
            log.trace("{0} is looking for a new backup to replace {1}", addr, leaver);
            result.add(addr);
         }
      }
      log.trace("Nodes that need new backups are: {0}", result);
      return result;
   }

   List<Address> receiversOfLeaverState(List<Address> stateProviders) {
      List<Address> result = new ArrayList<Address>();
      for (Address addr : stateProviders) {
         List<Address> addressList = consistentHash.getBackupsForNode(addr, getReplCount());
         result.add(addressList.get( addressList.size() - 1));         
      }
      log.trace("This node won't receive state");
      return result;
   }

   public boolean isLocal(Object key) {
      return consistentHash == null || consistentHash.isKeyLocalToAddress(self, key, getReplCount());
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

   public InternalCacheEntry retrieveFromRemoteSource(Object key) throws Exception {
      ClusteredGetCommand get = cf.buildClusteredGetCommand(key);

      ResponseFilter filter = new ClusteredGetResponseValidityFilter(locate(key));
      List<Response> responses = rpcManager.invokeRemotely(locate(key), get, ResponseMode.SYNCHRONOUS,
              configuration.getSyncReplTimeout(), false, filter);

      if (!responses.isEmpty()) {
         for (Response r : responses) {
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
      log.trace("Installing new consistent hash {0}", consistentHash);
      this.consistentHash = consistentHash;
   }

   public void setOldConsistentHash(ConsistentHash oldConsistentHash) {
      this.oldConsistentHash = oldConsistentHash;
   }

   @ManagedOperation(description = "Determines whether a given key is affected by an ongoing rehash, if any.")
   @Operation(displayName = "Could key be affected by rehash?")
   public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
      return transactionLogger.isEnabled() && oldConsistentHash != null && !oldConsistentHash.locate(key, getReplCount()).contains(self);
   }

   public TransactionLogger getTransactionLogger() {
      return transactionLogger;
   }

   public List<Address> requestPermissionToJoin(Address a) {
      if (JOINER_CAS.compareAndSet(this, null, a)) {
         log.trace("Allowing {0} to join", a);
         return new LinkedList<Address>(consistentHash.getCaches());
      } else {
         log.trace("Not alowing {0} to join since there is a join already in progress for node {1}", a, joiner);
         return null;
      }
   }

   public NodeTopologyInfo informRehashOnJoin(Address a, boolean starting, NodeTopologyInfo nodeTopologyInfo) {
      log.trace("Informed of a JOIN by {0}.  Starting? {1}", a, starting);
      if (!starting) {
         if (consistentHash instanceof UnionConsistentHash) {
            UnionConsistentHash uch = (UnionConsistentHash) consistentHash;
            consistentHash = uch.getNewConsistentHash();
            oldConsistentHash = null;
         }
         joiner = null;            
      } else {
         topologyInfo.addNodeTopologyInfo(a, nodeTopologyInfo);
         log.trace("Node topology info added({0}).  Topology info is {1}", nodeTopologyInfo, topologyInfo);         
         ConsistentHash chOld = consistentHash;
         if (chOld instanceof UnionConsistentHash) throw new RuntimeException("Not expecting a union CH!");
         oldConsistentHash = chOld;
         joiner = a;        
         ConsistentHash chNew = ConsistentHashHelper.createConsistentHash(configuration, chOld.getCaches(), topologyInfo, a);
         consistentHash = new UnionConsistentHash(chOld, chNew);
      }
      log.trace("New CH is {0}", consistentHash);
      return topologyInfo.getNodeTopologyInfo(rpcManager.getAddress());
   }
   
   @Override
   public void informRehashOnLeave(Address sender) {
      leaveAcks.lock();
      try {
         leaveRehashAcks.add(sender);         
         log.trace("Received and processed LEAVE_REHASH_END from " + sender);
         ackArrived.signalAll();
      }
      finally{
         leaveAcks.unlock();
      }    
   }

   public void applyState(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state) {
      log.trace("Apply state with " + state);
      for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
         if (consistentHash.locate(e.getKey(), configuration.getNumOwners()).contains(self)) {
            InternalCacheValue v = e.getValue();
            PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle());
            InvocationContext ctx = icc.createInvocationContext();
            ctx.setFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_SHARED_CACHE_STORE);
            interceptorChain.invoke(ctx, put);
         }
      }
      boolean unlocked = false;
      try {
         drainLocalTransactionLog();
         unlocked = true;
      } finally {
         if (!unlocked) transactionLogger.unlockAndDisable();
      }
   }

   @Listener
   public class ViewChangeListener {
      
      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         log.trace("view change received. Needs to re-join? " + e.isNeedsToRejoin());         
         boolean started;
         // how long do we wait for a startup?
         if (e.isNeedsToRejoin()) {
            try {
               join();
            } catch (Exception e1) {
               log.fatal("Unable to recover from a partition merge!", e1);
            }
         } else {

            try {
               started = startLatch.await(2, TimeUnit.MINUTES);
               if (started) rehash(e.getNewMembers(), e.getOldMembers());
               else log.warn("DistributionManager not started after waiting up to 2 minutes!  Not rehashing!");
            } catch (InterruptedException ie) {
               log.warn("View change interrupted; not rehashing!");
            }
         }
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
      return !leavers.isEmpty() || rehashInProgress;
   }

   public boolean isJoinComplete() {
      return joinComplete;
   }

   public void setJoinComplete(boolean joinComplete) {
      log.debug("Setting joinComplete to " + joinComplete + " for node " + rpcManager.getAddress());      
      this.joinComplete = joinComplete;
   }

   void drainLocalTransactionLog() {
      List<WriteCommand> c;
      while (transactionLogger.shouldDrainWithoutLock()) {
         c = transactionLogger.drain();
         apply(c);
      }

      c = transactionLogger.drainAndLock();
      apply(c);

      transactionLogger.unlockAndDisable();
   }

   private void apply(List<WriteCommand> c) {
      for (WriteCommand cmd : c) {
         InvocationContext ctx = icc.createInvocationContext();
         ctx.setFlags(Flag.SKIP_REMOTE_LOOKUP);
         ctx.setFlags(Flag.CACHE_MODE_LOCAL);
         interceptorChain.invoke(ctx, cmd);
      }
   }

   public List<Address> getAffectedNodes(Set<Object> affectedKeys) {
      if (affectedKeys == null || affectedKeys.isEmpty()) return Collections.emptyList();

      Set<Address> an = new HashSet<Address>();
      for (List<Address> addresses : locateAll(affectedKeys).values()) an.addAll(addresses);
      return new ArrayList<Address>(an);
   }

   public void applyRemoteTxLog(List<WriteCommand> txLogCommands) {
      apply(txLogCommands);
   }

   @ManagedOperation(description = "Tells you whether a given key is local to this instance of the cache.  Only works with String keys.")
   @Operation(displayName = "Is key local?")
   public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
      return isLocal(key);
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

   public void setSelf(Address self) {
      this.self = self;
   }

   public void setConfiguration(Configuration configuration) {
      this.configuration = configuration;
   }

   public TopologyInfo getTopologyInfo() {
      return topologyInfo;
   }

   public boolean awaitLeaveRehashAcks(List<Address> stateReceivers, long timeout) throws InterruptedException {
      long start = System.currentTimeMillis();
      boolean timeoutReached = false;
      boolean receivedAcks = false;

      leaveAcks.lock();
      try {
         while (!timeoutReached) {            
            receivedAcks = leaveRehashAcks.size() > 0 && stateReceivers.containsAll(leaveRehashAcks);
            if (receivedAcks)                
               break;            
            else               
               ackArrived.await(1000, TimeUnit.MILLISECONDS);            
            
            timeoutReached = (System.currentTimeMillis() - start) > timeout;
         }         
      } finally {
         leaveRehashAcks.clear();
         leaveAcks.unlock();
      }
      return !timeoutReached;
   }
}
