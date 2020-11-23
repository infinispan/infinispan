package org.infinispan.xsite.irac;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;

import net.jcip.annotations.GuardedBy;

/**
 * A manually trigger {@link IracManager} delegator.
 * <p>
 * The keys are only sent to the remote site if triggered.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ManualIracManager extends ControlledIracManager {

   @GuardedBy("this")
   private final Map<Object, Object> pendingKeys;
   @GuardedBy("this")
   private boolean enabled;
   @Inject KeyPartitioner keyPartitioner;

   private ManualIracManager(IracManager actual) {
      super(actual);
      pendingKeys = new HashMap<>();
   }

   public static ManualIracManager wrapCache(Cache<?, ?> cache) {
      IracManager iracManager = TestingUtil.extractComponent(cache, IracManager.class);
      if (iracManager instanceof ManualIracManager) {
         return (ManualIracManager) iracManager;
      }
      return TestingUtil.wrapComponent(cache, IracManager.class, ManualIracManager::new);
   }

   @Override
   public synchronized void trackUpdatedKey(int segment, Object key, Object lockOwner) {
      if (enabled) {
         pendingKeys.put(key, lockOwner);
      } else {
         super.trackUpdatedKey(segment, key, lockOwner);
      }
   }

   @Override
   public synchronized void requestState(Address origin, IntSet segments) {
      //send the state for the keys we have pending in this instance!
      asDefaultIracManager()
            .ifPresent(im -> pendingKeys.forEach((k, lo) -> im.sendStateIfNeeded(origin, segments, keyPartitioner.getSegment(k), k, lo)));
      super.requestState(origin, segments);
   }

   public synchronized void sendKeys() {
      pendingKeys.forEach((key, lockOwner) -> super.trackUpdatedKey(keyPartitioner.getSegment(key), key, lockOwner));
      pendingKeys.clear();
   }

   public synchronized void enable() {
      enabled = true;
   }

   public synchronized void disable(DisableMode disableMode) {
      enabled = false;
      switch (disableMode) {
         case DROP:
            pendingKeys.clear();
            break;
         case SEND:
            sendKeys();
            break;
      }

   }

   public enum DisableMode {
      SEND,
      DROP
   }
}
