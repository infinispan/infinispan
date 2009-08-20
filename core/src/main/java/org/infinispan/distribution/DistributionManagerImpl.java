package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import static org.infinispan.distribution.ConsistentHashHelper.createConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
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
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistributionManagerImpl implements DistributionManager {
   private final Log log = LogFactory.getLog(DistributionManagerImpl.class);
   private final boolean trace = log.isTraceEnabled();
   Configuration configuration;
   volatile ConsistentHash consistentHash, oldConsistentHash;
   Address self;
   CacheLoaderManager cacheLoaderManager;
   RpcManager rpcManager;
   CacheManagerNotifier notifier;
   int replCount;
   ViewChangeListener listener;
   CommandsFactory cf;
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
   ExecutorService rehashExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, rehashQueue, tf);

   TransactionLogger transactionLogger = new TransactionLoggerImpl();
   volatile boolean rehashInProgress = false;
   volatile Address joiner;
   static final AtomicReferenceFieldUpdater<DistributionManagerImpl, Address> JOINER_CAS =
         AtomicReferenceFieldUpdater.newUpdater(DistributionManagerImpl.class, Address.class, "joiner");
   private DataContainer dataContainer;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private volatile boolean joinTaskSubmitted = false;
   volatile boolean joinComplete = false;
   final List<Address> leavers = new CopyOnWriteArrayList<Address>();
   volatile Future<Void> leaveTaskFuture;

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
      replCount = configuration.getNumOwners();
      consistentHash = createConsistentHash(configuration, rpcManager.getTransport().getMembers());
      self = rpcManager.getTransport().getAddress();
      listener = new ViewChangeListener();
      notifier.addListener(listener);
      if (rpcManager.getTransport().getMembers().size() > 1) {
         JoinTask joinTask = new JoinTask(rpcManager, cf, configuration, transactionLogger, dataContainer, this);
         rehashExecutor.submit(joinTask);
      } else {
         joinComplete = true;
      }
      joinTaskSubmitted = true;
   }

   @Stop(priority = 20)
   public void stop() {
      notifier.removeListener(listener);
      rehashExecutor.shutdownNow();
      joinComplete = false;
   }

   final List<Address> diffAll(List<Address> l1, List<Address> l2) {
      List<Address> largerList = l1.size() > l2.size() ? l1 : l2;
      List<Address> smallerList = largerList == l1 ? l2 : l1;

      List<Address> list = new ArrayList<Address>(largerList);
      list.removeAll(smallerList);
      return list;
   }

   final Address diff(List<Address> l1, List<Address> l2) {
      List<Address> l = diffAll(l1, l2);
      return l.isEmpty() ? null : l.get(0);
   }


   public void rehash(List<Address> newMembers, List<Address> oldMembers) {
      boolean join = oldMembers == null || oldMembers.size() < newMembers.size();
      // on view change, we should update our view
      log.info("Detected a veiw change.  Member list changed from {0} to {1}", oldMembers, newMembers);

      if (join) {
         Address joiner = diff(newMembers, oldMembers);
         log.info("This is a JOIN event!  Wait for notification from new joiner " + joiner);
      } else {
         Address leaver = diff(newMembers, oldMembers);
         log.info("This is a LEAVE event!  Node {0} has just left", leaver);

         boolean willReceiveLeaverState = willReceiveLeaverState(leaver);
         boolean willSendLeaverState = willSendLeaverState(leaver);
         try {
            oldConsistentHash = consistentHash;
            consistentHash = ConsistentHashHelper.removeAddress(consistentHash, leaver, configuration);
         } catch (Exception e) {
            log.fatal("Unable to process leaver!!", e);
            throw new CacheException(e);
         }

         if (willReceiveLeaverState) {
            log.info("Starting transaction logging; expecting state from someone!");
            transactionLogger.enable();
         }

         if (willSendLeaverState) {
            if (leaveTaskFuture != null && (!leaveTaskFuture.isCancelled() || !leaveTaskFuture.isDone())) {
               leaveTaskFuture.cancel(true);
            }

            leavers.add(leaver);
            LeaveTask task = new LeaveTask(this, rpcManager, configuration, leavers, transactionLogger, cf, dataContainer);
            leaveTaskFuture = rehashExecutor.submit(task);

            log.info("Need to rehash");
         } else {
            log.info("Not in same subspace, so ignoring leave event");
         }
      }
   }

   boolean willSendLeaverState(Address leaver) {
      return consistentHash.isAdjacent(leaver, self);
   }

   boolean willReceiveLeaverState(Address leaver) {
      int dist = consistentHash.getDistance(leaver, self);
      return dist <= replCount;
   }

   public boolean isLocal(Object key) {
      return consistentHash.locate(key, replCount).contains(self);
   }

   public List<Address> locate(Object key) {
      return consistentHash.locate(key, replCount);
   }

   public Map<Object, List<Address>> locateAll(Collection<Object> keys) {
      return consistentHash.locateAll(keys, replCount);
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

   public boolean isAffectedByRehash(Object key) {
      if (transactionLogger.isEnabled() && oldConsistentHash != null && !oldConsistentHash.locate(key, replCount).contains(self)) {
         return true;
      } else {
         return false;
      }
   }

   public TransactionLogger getTransactionLogger() {
      return transactionLogger;
   }

   public List<Address> requestPermissionToJoin(Address joiner) {
      if (JOINER_CAS.compareAndSet(this, null, joiner))
         return new LinkedList<Address>(consistentHash.getCaches());
      else
         return null;
   }

   public void notifyJoinComplete(Address joiner) {
      log.trace("Received notification that {0} has completed a join.  Current 'joiner' flag is {1}, setting this to null.", joiner, this.joiner);
      if (this.joiner != null) {
         if (this.joiner.equals(joiner)) this.joiner = null;
      }
   }

   public void informRehashOnJoin(Address joiner, boolean starting) {
      log.trace("Informed of a JOIN by {0}.  Starting? {1}", joiner, starting);
      if (!starting) {
         if (consistentHash instanceof UnionConsistentHash) {
            UnionConsistentHash uch = (UnionConsistentHash) consistentHash;
            consistentHash = uch.getNewConsistentHash();
         }
         rehashInProgress = false;
      } else {
         ConsistentHash chOld = consistentHash;
         if (chOld instanceof UnionConsistentHash) throw new RuntimeException("Not expecting a union CH!");
         this.joiner = joiner;
         rehashInProgress = true;

         ConsistentHash chNew;
         try {
            chNew = (ConsistentHash) Util.getInstance(configuration.getConsistentHashClass());
         } catch (Exception e) {
            throw new CacheException("Unable to create instance of " + configuration.getConsistentHashClass(), e);
         }
         List<Address> newAddresses = new LinkedList<Address>(chOld.getCaches());
         newAddresses.add(joiner);
         chNew.setCaches(newAddresses);
         consistentHash = new UnionConsistentHash(chOld, chNew);
      }
      log.trace("New CH is {0}", consistentHash);
   }

   public void applyState(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state) {
      for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
         if (consistentHash.locate(e.getKey(), configuration.getNumOwners()).contains(self)) {
            InternalCacheValue v = e.getValue();
            PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle());
            InvocationContext ctx = icc.createInvocationContext();
            ctx.setFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_REMOTE_LOOKUP);
            interceptorChain.invoke(ctx, put);
         }
      }
   }

   @Listener
   public class ViewChangeListener {
      @ViewChanged
      public void handleViewChange(ViewChangedEvent e) {
         while (!joinTaskSubmitted) LockSupport.parkNanos(100 * 1000000);
         rehash(e.getNewMembers(), e.getOldMembers());
      }
   }

   public CacheStore getCacheStoreForRehashing() {
      if (cacheLoaderManager == null || !cacheLoaderManager.isEnabled() || cacheLoaderManager.isShared())
         return null;
      return cacheLoaderManager.getCacheStore();
   }

   public boolean isRehashInProgress() {
      return !leavers.isEmpty() || rehashInProgress;
   }

   public void applyReceivedState(Map<Object, InternalCacheValue> state) {
      applyState(consistentHash, state);
      boolean unlocked = false;
      try {
         drainTransactionLog();
         unlocked = true;
      } finally {
         if (!unlocked) transactionLogger.unlockAndDisable();
      }
   }

   void drainTransactionLog() {
      List<WriteCommand> c;
      while (transactionLogger.size() > 10) {
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
         interceptorChain.invoke(ctx, cmd);
      }
   }
}
