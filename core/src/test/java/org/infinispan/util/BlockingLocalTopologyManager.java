package org.infinispan.util;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;

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

   private final NotifierLatch blockConfirmRebalance;
   private final NotifierLatch blockConsistentHashUpdate;
   private final NotifierLatch blockReadConsistentHashUpdate;
   private final NotifierLatch blockWriteConsistentHashUpdate;
   private final NotifierLatch blockRebalanceStart;

   public BlockingLocalTopologyManager(LocalTopologyManager delegate) {
      super(delegate);
      blockRebalanceStart = new NotifierLatch();
      blockConsistentHashUpdate = new NotifierLatch();
      blockReadConsistentHashUpdate = new NotifierLatch();
      blockWriteConsistentHashUpdate = new NotifierLatch();
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

   public void stopBlockingAll() {
      for (LatchType type : LatchType.values()) {
         getLatch(type).stopBlocking();
      }
   }

   @Override
   protected final void beforeHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      getLatch(LatchType.CONSISTENT_HASH_UPDATE).blockIfNeeded();
   }

   @Override
   protected final void beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
      getLatch(LatchType.REBALANCE).blockIfNeeded();
   }

   @Override
   protected final void beforeConfirmRebalance(String cacheName, int topologyId, Throwable throwable) {
      getLatch(LatchType.CONFIRM_REBALANCE).blockIfNeeded();
   }

   @Override
   protected void beforeHandleReadCHUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      getLatch(LatchType.READ_CH_UPDATE).blockIfNeeded();
   }

   @Override
   protected void beforeHandleWriteCHUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      getLatch(LatchType.WRITE_CH_UPDATE).blockIfNeeded();
   }

   private NotifierLatch getLatch(LatchType type) {
      switch (type) {
         case CONSISTENT_HASH_UPDATE:
            return blockConsistentHashUpdate;
         case READ_CH_UPDATE:
            return blockReadConsistentHashUpdate;
         case WRITE_CH_UPDATE:
            return blockWriteConsistentHashUpdate;
         case CONFIRM_REBALANCE:
            return blockConfirmRebalance;
         case REBALANCE:
            return blockRebalanceStart;
      }
      throw new IllegalStateException("Should never happen!");
   }

   public static enum LatchType {
      CONSISTENT_HASH_UPDATE,
      WRITE_CH_UPDATE,
      READ_CH_UPDATE,
      CONFIRM_REBALANCE,
      REBALANCE
   }

}
