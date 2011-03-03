package org.infinispan.distribution;

import org.infinispan.CacheException;
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
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.MembershipArithmetic;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.infinispan.context.Flag.*;
import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

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
   private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Configuration configuration;
   private volatile ConsistentHash consistentHash, oldConsistentHash;
   private Address self;
   private CacheLoaderManager cacheLoaderManager;
   RpcManager rpcManager;
   private CacheManagerNotifier notifier;

   private ViewChangeListener listener;
   private CommandsFactory cf;

   private final ExecutorService rehashExecutor;

   private TransactionLogger transactionLogger;

   TopologyInfo topologyInfo = new TopologyInfo();

   /**
    * Rehash flag set by a rehash task associated with this DistributionManager
    */
   volatile boolean rehashInProgress = false;
   /**
    * https://issues.jboss.org/browse/ISPN-925 This makes sure that leavers list and consistent hash is updated
    * atomically.
    */
   private final ReentrantReadWriteLock chSwitchLock = new ReentrantReadWriteLock(true);
   /**
    * Address of a joiner node requesting to join Infinispan cluster. Each node in the cluster is aware of joiner's
    * identity. After joiner successfully joins (or fails to join), joiner field is nullified
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
   final List<Address> leavers = new CopyOnWriteArrayList<Address>();
   private volatile Future<Void> leaveTaskFuture;
   private final ReclosableLatch startLatch = new ReclosableLatch(false);

   private final Lock leaveAcksLock = new ReentrantLock();
   private final Condition acksArrived = leaveAcksLock.newCondition();
   private final Set<Address> leaveRehashAcks = new CopyOnWriteArraySet<Address>(); // this needs to be threadsafe!

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
      if (trace) log.trace("Starting distribution manager on " + getMyAddress());
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
         JoinTask joinTask = new JoinTask(rpcManager, cf, configuration, dataContainer, this, inboundInvocationHandler);
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
      log.info("Detected a view change.  Member list changed from %s to %s", oldMembers, newMembers);

      if (join) {
         Address joiner = MembershipArithmetic.getMemberJoined(oldMembers, newMembers);
         log.info("This is a JOIN event!  Wait for notification from new joiner " + joiner);
      } else {
         Address leaver = MembershipArithmetic.getMemberLeft(oldMembers, newMembers);
         log.info("This is a LEAVE event!  Node %s has just left", leaver);


         try {
            if (!(consistentHash instanceof UnionConsistentHash)) {
               oldConsistentHash = consistentHash;
            } else {
               oldConsistentHash = ((UnionConsistentHash) consistentHash).getNewConsistentHash();
            }
            addLeaverAndUpdatedConsistentHash(leaver);
         } catch (Exception e) {
            log.fatal("Unable to process leaver!!", e);
            throw new CacheException(e);
         }

         List<Address> stateProviders = holdersOfLeaversState(leaver);
         List<Address> receiversOfLeaverState = receiversOfLeaverState(stateProviders);
         boolean willReceiveLeaverState = receiversOfLeaverState.contains(self);
         boolean willProvideState = stateProviders.contains(self);
         if (willReceiveLeaverState || willProvideState) {
            log.info("I %s am participating in rehash, state providers %s, state receivers %s",
                     rpcManager.getTransport().getAddress(), stateProviders, receiversOfLeaverState);

            transactionLogger.enable();

            if (leaveTaskFuture != null
                  && (!leaveTaskFuture.isCancelled() || !leaveTaskFuture.isDone())) {
               if (trace) log.trace("Canceling running leave task!");
               leaveTaskFuture.cancel(true);
            }

            InvertedLeaveTask task = new InvertedLeaveTask(this, rpcManager, configuration, cf, dataContainer,
                                                           stateProviders, receiversOfLeaverState, willReceiveLeaverState);
            leaveTaskFuture = rehashExecutor.submit(task);
         } else {
            log.info("Not in same subspace, so ignoring leave event");
            topologyInfo.removeNodeInfo(leaver);
            removeLeaver(leaver);
         }
      }
   }

   public List<Address> getLeavers() {
      chSwitchLock.readLock().lock();
      try {
         return Collections.unmodifiableList(leavers);
      } finally {
         chSwitchLock.readLock().unlock();
      }
   }

   private void addLeaverAndUpdatedConsistentHash(Address leaver) {
      chSwitchLock.writeLock().lock();
      try {
         leavers.add(leaver);
         if (trace) log.trace("Added new leaver %s, leavers list is %s", leaver, leavers);
         consistentHash = ConsistentHashHelper.removeAddress(consistentHash, leaver, configuration, topologyInfo);
      } finally {
         chSwitchLock.writeLock().unlock();
      }
   }

   public boolean removeLeaver(Address leaver) {
      chSwitchLock.writeLock().lock();
      try {
         //if we are affected by the rehash then levers are removed within the InvertedLeaveTask
         return leavers.remove(leaver);
      } finally {
         if (trace) log.trace("After removing leaver[ %s ] leavers list is %s", leaver, leavers);
         chSwitchLock.writeLock().unlock();
      }
   }

   List<Address> holdersOfLeaversState(Address leaver) {
      List<Address> result = new ArrayList<Address>();
      for (Address addr : oldConsistentHash.getCaches()) {
         List<Address> backups = oldConsistentHash.getBackupsForNode(addr, getReplCount());
         if (addr.equals(leaver)) {
            if (backups.size() > 1) {
               Address mainBackup = backups.get(1);
               result.add(mainBackup);
               if (trace)
                  log.trace("Leaver %s main backup %s is looking for another backup as well.", leaver, mainBackup);
            }
         } else if (backups.contains(leaver)) {
            if (trace) log.trace("%s is looking for a new backup to replace leaver %s", addr, leaver);
            result.add(addr);
         }
      }
      if (trace) log.trace("Nodes that need new backups are: %s", result);
      return result;
   }

   List<Address> receiversOfLeaverState(List<Address> stateProviders) {
      List<Address> result = new ArrayList<Address>();
      for (Address addr : stateProviders) {
         List<Address> addressList = consistentHash.getBackupsForNode(addr, getReplCount());
         result.add(addressList.get(addressList.size() - 1));
      }
      if (trace) log.trace("This node won't receive state");
      return result;
   }

   @Deprecated
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
      if (trace) log.trace("Installing new consistent hash %s", consistentHash);
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

   public Set<Address> requestPermissionToJoin(Address a) {
      try {
         if (!startLatch.await(5, TimeUnit.MINUTES)) {
            log.warn("DistributionManager not started after waiting up to 5 minutes!  Not rehashing!");
            return null;
         }
      } catch (InterruptedException e) {
         // Nothing to do here
         Thread.currentThread().interrupt();
      }

      if (JOINER_CAS.compareAndSet(this, null, a)) {
         if (trace) log.trace("Allowing %s to join", a);
         return new HashSet<Address>(consistentHash.getCaches());
      } else {
         if (trace)
            log.trace("Not alowing %s to join since there is a join already in progress for node %s", a, joiner);
         return null;
      }
   }

   public NodeTopologyInfo informRehashOnJoin(Address a, boolean starting, NodeTopologyInfo nodeTopologyInfo) {
      if (trace) log.trace("Informed of a JOIN by %s.  Starting? %s", a, starting);
      if (!starting) {
         if (consistentHash instanceof UnionConsistentHash) {
            UnionConsistentHash uch = (UnionConsistentHash) consistentHash;
            consistentHash = uch.getNewConsistentHash();
            oldConsistentHash = null;
         }
         joiner = null;
      } else {
         topologyInfo.addNodeTopologyInfo(a, nodeTopologyInfo);
         if (trace) log.trace("Node topology info added(%s).  Topology info is %s", nodeTopologyInfo, topologyInfo);
         ConsistentHash chOld = consistentHash;
         if (chOld instanceof UnionConsistentHash) throw new RuntimeException("Not expecting a union CH!");
         oldConsistentHash = chOld;
         joiner = a;
         ConsistentHash chNew = ConsistentHashHelper.createConsistentHash(configuration, chOld.getCaches(), topologyInfo, a);
         consistentHash = new UnionConsistentHash(chOld, chNew);
      }
      if (trace) log.trace("New CH is %s", consistentHash);
      return topologyInfo.getNodeTopologyInfo(rpcManager.getAddress());
   }

   @Override
   public void informRehashOnLeave(Address sender) {
      leaveAcksLock.lock();
      try {
         leaveRehashAcks.add(sender);
         if (trace)
            log.trace("%s has been informed that %s has completed applying state sent from %s as a part of a LEAVE_REHASH.", self, sender, self);
         acksArrived.signalAll();
      } finally {
         leaveAcksLock.unlock();
      }
   }

   private Map<Object, InternalCacheValue> applyStateMap(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state, boolean withRetry) {
      Map<Object, InternalCacheValue> retry = withRetry ? new HashMap<Object, InternalCacheValue>() : null;
      for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
         if (consistentHash.locate(e.getKey(), configuration.getNumOwners()).contains(self)) {
            InternalCacheValue v = e.getValue();
            InvocationContext ctx = icc.createInvocationContext();
            ctx.setFlags(CACHE_MODE_LOCAL, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING); // locking not necessary in the case of a join since the node isn't doing anything else.

            try {
               PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle(), ctx.getFlags());
               interceptorChain.invoke(ctx, put);
            } catch (Exception ee) {
               if (withRetry) {
                  if (trace)
                     log.trace("Problem %s encountered when applying state for key %s. Adding entry to retry queue.", ee.getMessage(), e.getKey());
                  retry.put(e.getKey(), e.getValue());
               } else {
                  log.warn("Problem %s encountered when applying state for key %s!", ee.getMessage(), e.getKey());
               }
            }
         }
      }
      return retry;
   }

   public void applyState(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state, RemoteTransactionLogger tlog, boolean forLeave) {
      if (trace) log.trace("Applying the following keys: %s", state.keySet());

      int retryCount = 3; // in case we have issues applying state.
      Map<Object, InternalCacheValue> pendingApplications = state;
      for (int i = 0; i < retryCount; i++) {
         pendingApplications = applyStateMap(consistentHash, pendingApplications, true);
         if (pendingApplications.isEmpty()) break;
      }
      // one last go
      if (!pendingApplications.isEmpty()) applyStateMap(consistentHash, pendingApplications, false);

      if (!forLeave) drainLocalTransactionLog(tlog);

      if (trace) log.trace("%s has completed applying state", self);
   }

   public void setRehashInProgress(boolean value) {
      rehashInProgress = value;
   }

   @Listener
   public class ViewChangeListener {

      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         if (trace) log.trace("view change received. Needs to re-join? " + e.isNeedsToRejoin());
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
               started = startLatch.await(5, TimeUnit.MINUTES);
               if (started) rehash(e.getNewMembers(), e.getOldMembers());
               else log.warn("DistributionManager not started after waiting up to 5 minutes!  Not rehashing!");
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
      chSwitchLock.readLock().lock();
      boolean nodeLeaving;
      try {
         nodeLeaving = !leavers.isEmpty();
         if (trace)
            log.trace("Node leaving? %s RehashInProgress? %s Leavers = %s", nodeLeaving, rehashInProgress, leavers);
      } finally {
         chSwitchLock.readLock().unlock();
      }
      return nodeLeaving || rehashInProgress;
   }

   public void markLeaverAsHandled(Address leaver) {
      chSwitchLock.writeLock().lock();
      try {
         leavers.remove(leaver);
         topologyInfo.removeNodeInfo(leaver);
      } finally {
         chSwitchLock.writeLock().unlock();
      }

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
         if (trace) log.trace("Draining %s entries from transaction log", c.size());
         applyRemoteTxLog(c);
      }

      boolean unlock = acquireDistSyncLock();
      try {
         this.enteredFinalJoinPhase = true;
         c = tlog.drainAndLock(null);
         if (trace) log.trace("Locked and draining %s entries from transaction log", c.size());
         applyRemoteTxLog(c);

         Collection<PrepareCommand> pendingPrepares = tlog.getPendingPrepares();
         if (trace) log.trace("Applying %s pending prepares", pendingPrepares.size());
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
         log.info("Couldn't acquire shared lock");
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return false;
   }

   public List<Address> getAffectedNodes(Set<Object> affectedKeys) {
      if (affectedKeys == null || affectedKeys.isEmpty()) {
         if (log.isTraceEnabled()) log.trace("Affected keys are empty");
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
            log.warn("Caught exception replaying %s", e, cmd);
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

   public boolean awaitLeaveRehashAcks(Set<Address> stateReceivers, long timeout) throws InterruptedException {
      long start = System.currentTimeMillis();
      boolean timeoutReached = false;
      boolean receivedAcks;

      leaveAcksLock.lock();
      try {
         while (!timeoutReached) {
            receivedAcks = stateReceivers.equals(leaveRehashAcks);
            if (receivedAcks)
               break;
            else
               acksArrived.await(1000, TimeUnit.MILLISECONDS);

            timeoutReached = (System.currentTimeMillis() - start) > timeout;
         }
      } finally {
         leaveRehashAcks.clear();
         leaveAcksLock.unlock();
      }
      return !timeoutReached;
   }
}
