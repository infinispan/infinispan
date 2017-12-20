package org.infinispan.util;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Replaces the LocalTopologyManager and allows it to block the phases of the state transfer:
 * <ul>
 *    <li>Rebalance Start</li>
 *    <li>Confirm Rebalance</li>
 *    <li>Consistent Hash Update</li>
 * </ul>
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class BlockingLocalTopologyManager extends AbstractControlledLocalTopologyManager {
   private static final Log log = LogFactory.getLog(BlockingLocalTopologyManager.class);
   private static final int TIMEOUT_SECONDS = 10;

   private final Address address;
   private final String expectedCacheName;
   private final BlockingQueue<Event> queuedTopologies = new LinkedBlockingQueue<>();
   private volatile boolean enabled = true;
   private volatile RuntimeException exception;

   private BlockingLocalTopologyManager(LocalTopologyManager delegate, Address address, String cacheName) {
      super(delegate);
      this.address = address;
      this.expectedCacheName = cacheName;
   }

   public static BlockingLocalTopologyManager replaceTopologyManager(EmbeddedCacheManager cacheContainer,
                                                                     String cacheName) {
      LocalTopologyManager manager = TestingUtil.extractGlobalComponent(cacheContainer, LocalTopologyManager.class);
      BlockingLocalTopologyManager controlledLocalTopologyManager =
         new BlockingLocalTopologyManager(manager, cacheContainer.getAddress(), cacheName);
      TestingUtil.replaceComponent(cacheContainer, LocalTopologyManager.class, controlledLocalTopologyManager, true);
      return controlledLocalTopologyManager;
   }

   public static BlockingLocalTopologyManager replaceTopologyManagerDefaultCache(EmbeddedCacheManager cacheContainer) {
      return replaceTopologyManager(cacheContainer, CacheContainer.DEFAULT_CACHE_NAME);
   }

   public static void confirmTopologyUpdate(CacheTopology.Phase phase, BlockingLocalTopologyManager... topologyManagers)
      throws InterruptedException {
      for (BlockingLocalTopologyManager topologyManager : topologyManagers) {
         topologyManager.confirmTopologyUpdate(phase);
      }
   }

   public BlockedTopology expectTopologyUpdate(CacheTopology.Phase phase) throws InterruptedException {
      BlockedTopology blockedTopology = expectTopologyUpdate();
      assertTrue("Expected a CH_UPDATE or REBALANCE_START, but got a CONFIRMATION",
                 blockedTopology.getType() != Type.CONFIRMATION);
      assertEquals(phase, blockedTopology.getCacheTopology().getPhase());
      return blockedTopology;
   }

   public BlockedTopology expectTopologyUpdate(CacheTopology.Phase phase, int topologyId) throws InterruptedException {
      BlockedTopology blockedTopology = expectTopologyUpdate();
      assertEquals(topologyId, blockedTopology.getCacheTopology().getTopologyId());
      assertEquals(phase, blockedTopology.getCacheTopology().getPhase());
      return blockedTopology;
   }

   public BlockedTopology expectTopologyUpdate() throws InterruptedException {
      Event update = queuedTopologies.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (update == null) {
         throw new TimeoutException("Timed out waiting for topology update on " + address);
      }
      return new BlockedTopology(update);
   }

   public BlockedTopology expectCHUpdate() throws InterruptedException {
      BlockedTopology blockedTopology = expectTopologyUpdate();
      assertEquals(Type.CH_UPDATE, blockedTopology.getType());
      return blockedTopology;
   }

   public BlockedTopology expectCHUpdate(int topologyId) throws InterruptedException {
      BlockedTopology blockedTopology = expectCHUpdate();
      assertEquals(topologyId, blockedTopology.getCacheTopology().getTopologyId());
      return blockedTopology;
   }

   public BlockedTopology expectRebalanceStart() throws InterruptedException {
      BlockedTopology blockedTopology = expectTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL);
      assertEquals(Type.REBALANCE_START, blockedTopology.getType());
      return blockedTopology;
   }

   public BlockedTopology expectRebalanceStart(int topologyId) throws InterruptedException {
      BlockedTopology blockedTopology = expectRebalanceStart();
      assertEquals(topologyId, blockedTopology.getCacheTopology().getTopologyId());
      return blockedTopology;
   }

   public BlockedConfirmation expectPhaseConfirmation() throws InterruptedException {
      Event update = queuedTopologies.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (update == null) {
         throw new TimeoutException("Timed out waiting for phase confirmation on " + address);
      }
      assertEquals(Type.CONFIRMATION, update.type);
      return new BlockedConfirmation(update);
   }

   public BlockedConfirmation expectPhaseConfirmation(int topologyId) throws InterruptedException {
      BlockedConfirmation blockedConfirmation = expectPhaseConfirmation();
      assertEquals(topologyId, blockedConfirmation.getTopologyId());
      return blockedConfirmation;
   }

   /**
    * Expect a topology updates and unblock it.
    * <p>
    * If the update requires confirmation, unblock the confirmation as well.
    */
   public void confirmTopologyUpdate(CacheTopology.Phase phase) throws InterruptedException {
      expectTopologyUpdate(phase).unblock();
      if (needConfirmation(phase)) {
         expectPhaseConfirmation().unblock();
      }
   }

   public void expectNoTopologyUpdate(long timeout, TimeUnit timeUnit) throws InterruptedException {
      Event update = queuedTopologies.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (update != null) {
         throw new TestException(
            "Expected no topology update on " + address + ", but got " + update.type + " " + update.topologyId);
      }
   }

   private boolean needConfirmation(CacheTopology.Phase phase) {
      return phase == CacheTopology.Phase.TRANSITORY ||
                phase == CacheTopology.Phase.READ_OLD_WRITE_ALL ||
                phase == CacheTopology.Phase.READ_ALL_WRITE_ALL ||
                phase == CacheTopology.Phase.READ_NEW_WRITE_ALL;
   }

   public void stopBlocking() {
      if (exception != null) {
         throw exception;
      }
      if (!queuedTopologies.isEmpty()) {
         log.error("Stopped blocking topology updates, but there are " + queuedTopologies.size() +
                      " blocked updates in the queue: " + queuedTopologies);
      }
      enabled = false;
   }

   @Override
   protected final void beforeHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      if (!enabled || !expectedCacheName.equals(cacheName))
         return;

      Event event = new Event(cacheTopology, cacheTopology.getTopologyId(), viewId,
                              Type.CH_UPDATE);
      queuedTopologies.add(event);
      log.debugf("Blocking topology update for cache %s: %s", cacheName, cacheTopology);
      event.awaitUnblock();
      log.debugf("Continue consistent hash update for cache %s: %s", cacheName, cacheTopology);
   }

   @Override
   protected final void beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
      if (!expectedCacheName.equals(cacheName))
         return;

      Event event = new Event(cacheTopology, cacheTopology.getTopologyId(), viewId,
                              Type.REBALANCE_START);
      queuedTopologies.add(event);
      log.debugf("Blocking rebalance start for cache %s: %s", cacheName, cacheTopology);
      event.awaitUnblock();
      log.debugf("Continue rebalance start for cache %s: %s", cacheName, cacheTopology);
   }

   @Override
   protected final void beforeConfirmRebalancePhase(String cacheName, int topologyId, Throwable throwable) {
      if (!expectedCacheName.equals(cacheName))
         return;

      Event event = new Event(null, topologyId, -1, Type.CONFIRMATION);
      queuedTopologies.add(event);
      log.debugf("Blocking rebalance confirmation for cache %s: %s", cacheName, topologyId);
      event.awaitUnblock();
      log.debugf("Continue rebalance confirmation for cache %s: %s", cacheName, topologyId);
   }

   void failManager(Throwable e) {
      if (e instanceof RuntimeException) {
         exception = (RuntimeException) e;
      } else {
         exception = new TestException(e);
      }
   }

   public enum Type {CH_UPDATE, REBALANCE_START, CONFIRMATION}

   class Event {
      final CacheTopology cacheTopology;
      final int topologyId;
      final int viewId;
      final Type type;

      private final CompletableFuture<Void> latch = new CompletableFuture<>();

      Event(CacheTopology cacheTopology, int topologyId, int viewId, Type type) {
         this.cacheTopology = cacheTopology;
         this.topologyId = topologyId;
         this.viewId = viewId;
         this.type = type;
      }

      void awaitUnblock() {
         try {
            latch.get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e);
         } catch (ExecutionException e) {
            fail(e.getCause());
         } catch (java.util.concurrent.TimeoutException e) {
            fail(e);
         }
      }

      void unblock() {
         if (latch.isCompletedExceptionally()) {
            latch.join();
         }
         log.tracef("Unblocking %s %d on %s", type, topologyId, address);
         latch.complete(null);
      }

      void fail(Throwable e) {
         if (latch.isCompletedExceptionally()) {
            latch.join();
         }
         log.errorf(e, "Failed waiting for test to unblock %s %d on %s", type, topologyId, address);
         failManager(e);
         latch.completeExceptionally(e);
      }

      @Override
      public String toString() {
         return "Event{" +
                   "type=" + type +
                   ", topologyId=" + topologyId +
                   ", viewId=" + viewId +
                   '}';
      }
   }

   public class BlockedTopology {
      private Event event;

      BlockedTopology(Event event) {
         this.event = event;
      }

      public CacheTopology getCacheTopology() {
         return event.cacheTopology;
      }

      public int getViewId() {
         return event.viewId;
      }

      public Type getType() {
         return event.type;
      }

      public void unblock() {
         event.unblock();
      }
   }

   public class BlockedConfirmation {
      private Event event;

      BlockedConfirmation(Event event) {
         this.event = event;
      }

      public int getTopologyId() {
         return event.topologyId;
      }

      public void unblock() {
         event.unblock();
      }
   }
}
