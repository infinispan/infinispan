package org.infinispan.query.indexmanager;

import net.jcip.annotations.GuardedBy;
import org.hibernate.search.backend.BackendFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the current state of being a "master" node or a node delegating index
 * update operations to other nodes.
 * In a static cluster this would have been a boolean state, but a state machine
 * is modelled here to cope with transitions between:
 *
 * Initialization of a node - still not having enough information on the cluster
 * Becoming a master because of previous master failure / shutdown
 * Forfaiting the master role (useful for cluster merges)
 *
 * The transition to become a master goes via different phases, and at each state
 * the process is reversible. So for example if operations have been put on hold
 * while the node is being upgraded, but then the master election is moved to a
 * different node quickly (cluster startup scenario), the buffered operations
 * will be forwarded to the last backend.
 * A node being forwarded update operations but not being the master anymore,
 * will re-forward the payload to the new master: stability by induction.
 *
 * The solution is rather poor at managing cluster Merge operations, but we
 * need to build on upcoming functionality from Infinispan core for that; for
 * example the index content wouldn't be consistent either so one would likely
 * need to wipe the index and rebuild it.
 * Also we're dealing with the inherent limitation of a "cluster wide lock"
 * concept not being compatible with sub-groups of nodes in which a new lead
 * might be elected and a lock per group might have been created.
 *
 * A lock cleanup is not too aggressive: in case a stale lock is detected,
 * scheduled work is postponed. This implies that in such situations in which
 * a stale lock needs to be cleaned up, index operations might not be visible
 * to the transaction committer.
 * I've chosen for this option as the lesser evil vs. blocking incoming RPCs,
 * although if the buffer for postponed operations gets filled too quickly,
 * we'll both speed up the lock acquisition and apply backpressure to the clients.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2014 Red Hat Inc.
 * @since 7.0
 */
@Listener
final class ClusteredSwitchingBackend implements LazyInitializableBackend {

   private static final Log log = LogFactory.getLog(ClusteredSwitchingBackend.class, Log.class);

   /**
    * Each attempt introduces approximately 10 seconds delay, and waiting
    * more should never be reasonable as it means we're handling a cluster merge.
    * Infinispan doesn't currently handle merges, so in that case the index
    * is probably corrupted: no point in keeping the lock either.
    * The only reason to wait for it is to handle very brief merges caused by
    * occasional high load, or in case users are writing directly to the index.
    * Sustained direct writes to the index should not be done either, at least
    * not without disabling index exclusivity which implies the lock will be
    * available in a shorter time.
    */
   private static final int MAX_LOCK_ACQUISITION_ATTEMPTS = 2;

   private final Address localAddress;
   private final RpcManager rpcManager;
   private final LocalBackendFactory factory;
   private final IndexLockController indexlock;
   private final boolean async;

   private final String indexName;
   private final String cacheName;

   /**
    * Monotonically increasing view identification sequence:
    * we use it to ignore stale events.
    * FIXME: why are ids just an int? Is that going to be enough?
    */
   private final AtomicInteger lastSeenViewId = new AtomicInteger(-1);

   private volatile Address currentMaster;
   private volatile IndexingBackend currentBackend;

   @GuardedBy("this")
   private boolean initialized = false;

   @GuardedBy("this")
   private int masterLockAcquisitionAttempts = 0;

   ClusteredSwitchingBackend(Properties props, ComponentRegistry componentsRegistry, String indexName, LocalBackendFactory factory, IndexLockController indexlock) {
      this.indexName = indexName;
      this.factory = factory;
      this.indexlock = indexlock;
      this.rpcManager = componentsRegistry.getComponent(RpcManager.class);
      this.cacheName = componentsRegistry.getCacheName();
      if (rpcManager == null) {
         throw new IllegalStateException("This Cache is not clustered! The switching backend should not be used for local caches");
      }
      this.localAddress = rpcManager.getAddress();
      this.currentBackend = new LazyInitializingBackend(this);
      this.async = !BackendFactory.isConfiguredAsSync(props);
   }

   @ViewChanged
   public void viewChanged(final ViewChangedEvent e) {
      final int currentViewId = lastSeenViewId.get();
      final int viewId = e.getViewId();
      if (viewId > currentViewId) {
         if (lastSeenViewId.compareAndSet(currentViewId, viewId)) {
            applyViewChangedEvent(e);
         }
      }
   }

   @Override
   public void initialize() {
      // we use lazyInitialize() to postpone operations to last minute:
      // avoids unnecessary elections while the initial cluster is formed.
   }

   @Override
   public synchronized void lazyInitialize() {
      if (initialized) {
         return;
      }
      this.initialized = true;
      final List<Address> members = rpcManager.getMembers();
      assert members != null;
      assert members.size() > 0;
      assert members.get(0) != null;
      final Address initialMaster = members.get(0);
      lastSeenViewId.set(rpcManager.getTransport().getViewId());
      if (thisIsNewMaster(initialMaster)) {
         acquireControlStart();
      } else {
         updateRoutingToNewRemote(initialMaster);
      }
   }

   private synchronized void applyViewChangedEvent(ViewChangedEvent e) {
      assert e != null;
      assert e.getNewMembers().size() > 0;
      assert e.getNewMembers().get(0) != null;
      if (log.isDebugEnabled()) {
         log.debug("Notified of new View! Members: " + e.getNewMembers());
      }
      final Address newmaster = e.getNewMembers().get(0);
      if (masterDidChange(newmaster)) {
         if (thisIsMaster()) {
            if (log.isDebugEnabled()) {
               log.debug("No longer a MASTER node, releasing the index lock.");
            }
            forfeitControl(newmaster);
         } else if (thisIsNewMaster(newmaster)) {
            log.debug("Electing SELF as MASTER!");
            acquireControlStart();
         } else {
            updateRoutingToNewRemote(newmaster);
            if (log.isDebugEnabled()) {
               log.debug("New master elected, now routing updates to node " + newmaster);
            }
         }
      }
   }

   private boolean thisIsNewMaster(Address newmaster) {
      return localAddress.equals(newmaster);
   }

   private boolean thisIsMaster() {
      return localAddress.equals(currentMaster);
   }

   private boolean masterDidChange(final Address newmaster) {
      if (newmaster == null) {
         return false;
      } else {
         return !newmaster.equals(currentMaster);
      }
   }

   private void updateRoutingToNewRemote(final Address newMaster) {
      final IndexingBackend newBackend = new RemoteIndexingBackend(cacheName, rpcManager, indexName, newMaster, async);
      swapNewBackendIn(newBackend, newMaster);
   }

   private void acquireControlStart() {
      final IndexingBackend backend = new LockAcquiringBackend(this);
      this.masterLockAcquisitionAttempts = 0;
      swapNewBackendIn(backend, localAddress);
   }

   private void forfeitControl(Address newMasterAddress) {
      final IndexingBackend newBackend = new RemoteIndexingBackend(cacheName, rpcManager, indexName, newMasterAddress, async);
      swapNewBackendIn(newBackend, newMasterAddress);
   }

   private void swapNewBackendIn(IndexingBackend newBackend, Address newMasterAddress) {
      final IndexingBackend oldBackend = currentBackend;
      log.debugv("Swapping from backend {0} to {1}'", oldBackend, newBackend);
      this.currentBackend = newBackend;
      this.currentMaster = newMasterAddress;
      closeBackend(oldBackend, currentBackend);
   }

   @Override
   public void shutdown() {
      closeBackend(currentBackend, null);
      this.currentBackend = null;
   }

   @Override
   public IndexingBackend getCurrentIndexingBackend() {
      return currentBackend;
   }

   private static void closeBackend(final IndexingBackend oldOne, final IndexingBackend replacement) {
      if (oldOne != null) {
         oldOne.flushAndClose(replacement);
      }
   }

   @Override
   public synchronized boolean attemptUpgrade(IndexingBackend expectedBackend) {
      log.trace("owning lock for attemptUpgrade(IndexingBackend)");
      if (currentBackend != expectedBackend) {
         //This needs to be checked while holding the lock
         return true;
      }
      if (masterLockAcquisitionAttempts >= MAX_LOCK_ACQUISITION_ATTEMPTS) {
         indexlock.forceLockClear();
         swapNewBackendIn(factory.createLocalIndexingBackend(), localAddress);
         return true;
      } else {
         masterLockAcquisitionAttempts++;
      }
      if (indexlock.waitForAvailability()) {
         swapNewBackendIn(factory.createLocalIndexingBackend(), localAddress);
         return true;
      } else {
         log.trace("Index lock not available: index update operations postponed.");
         return false;
      }
   }

}
