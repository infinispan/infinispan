package org.infinispan.util;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
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

   private final Log log = LogFactory.getLog(BlockingLocalTopologyManager.class);
   private final NotifierLatch blockConfirmRebalance;
   private final NotifierLatch blockConsistentHashUpdate;
   private final NotifierLatch blockRebalanceStart;

   public BlockingLocalTopologyManager(LocalTopologyManager delegate) {
      super(delegate);
      blockRebalanceStart = new NotifierLatch();
      blockConsistentHashUpdate = new NotifierLatch();
      blockConfirmRebalance = new NotifierLatch();
   }

   public static BlockingLocalTopologyManager replaceTopologyManager(CacheContainer cacheContainer) {
      LocalTopologyManager manager = TestingUtil.extractGlobalComponent(cacheContainer, LocalTopologyManager.class);
      BlockingLocalTopologyManager controlledLocalTopologyManager = new BlockingLocalTopologyManager(manager);
      TestingUtil.replaceComponent(cacheContainer, LocalTopologyManager.class, controlledLocalTopologyManager, true);
      return controlledLocalTopologyManager;
   }

   public void startBlocking(LatchType type) {
      getLatch(type).startBlocking();
   }

   public void stopBlocking(LatchType type) {
      getLatch(type).stopBlocking();
   }

   public void waitToBlock(LatchType type) throws InterruptedException {
      getLatch(type).waitToBlock();
   }

   public void unblockOnce(LatchType type) {
      getLatch(type).unblockOnce();
   }

   public void waitToBlockAndUnblockOnce(LatchType type) throws InterruptedException {
      getLatch(type).waitToBlockAndUnblockOnce();
   }

   public void stopBlockingAll() {
      for (LatchType type : LatchType.values()) {
         getLatch(type).stopBlocking();
      }
   }

   @Override
   protected final void beforeHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      log.debugf("Before consistent hash update %s", cacheTopology);
      getLatch(LatchType.CONSISTENT_HASH_UPDATE).blockIfNeeded();
      log.debugf("Continue consistent hash update %s", cacheTopology);
   }

   @Override
   protected final void beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
      log.debugf("Before rebalance %s", cacheTopology);
      getLatch(LatchType.REBALANCE).blockIfNeeded();
      log.debugf("Continue rebalance %s", cacheTopology);
   }

   @Override
   protected final void beforeConfirmRebalancePhase(String cacheName, int topologyId, Throwable throwable) {
      log.debugf("Before confirm topology %d", topologyId);
      getLatch(LatchType.CONFIRM_REBALANCE_PHASE).blockIfNeeded();
      log.debugf("Continue confirm topology %d", topologyId);
   }

   private NotifierLatch getLatch(LatchType type) {
      switch (type) {
         case CONSISTENT_HASH_UPDATE:
            return blockConsistentHashUpdate;
         case CONFIRM_REBALANCE_PHASE:
            return blockConfirmRebalance;
         case REBALANCE:
            return blockRebalanceStart;
      }
      throw new IllegalStateException("Should never happen!");
   }

   public static enum LatchType {
      CONSISTENT_HASH_UPDATE,
      CONFIRM_REBALANCE_PHASE,
      REBALANCE
   }

}
