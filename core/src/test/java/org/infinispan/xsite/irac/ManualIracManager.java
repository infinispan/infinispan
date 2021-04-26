package org.infinispan.xsite.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.statetransfer.XSiteState;

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
   private final List<StateTransferRequest> pendingStateTransfer;
   @Inject KeyPartitioner keyPartitioner;

   private ManualIracManager(IracManager actual) {
      super(actual);
      pendingKeys = new HashMap<>();
      pendingStateTransfer = new ArrayList<>(2);
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
   public synchronized CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      if (enabled) {
         StateTransferRequest request = new StateTransferRequest(stateList);
         pendingStateTransfer.add(request);
         return request;
      } else {
         return super.trackForStateTransfer(stateList);
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
      pendingStateTransfer.forEach(request -> {
         CompletionStage<Void> rsp = super.trackForStateTransfer(request.state);
         rsp.whenComplete(request);
      });
      pendingStateTransfer.clear();
   }

   public synchronized void enable() {
      enabled = true;
   }

   public synchronized void disable(DisableMode disableMode) {
      enabled = false;
      switch (disableMode) {
         case DROP:
            pendingKeys.clear();
            pendingStateTransfer.clear();
            break;
         case SEND:
            sendKeys();
            break;
      }

   }

   public boolean isEmpty() {
      return asDefaultIracManager().map(DefaultIracManager::isEmpty).orElse(true);
   }

   public enum DisableMode {
      SEND,
      DROP
   }

   private static class StateTransferRequest extends CompletableFuture<Void> implements BiConsumer<Void, Throwable> {
      private final Collection<XSiteState> state;

      private StateTransferRequest(Collection<XSiteState> state) {
         this.state = new ArrayList<>(state);
      }


      @Override
      public void accept(Void unused, Throwable throwable) {
         if (throwable != null) {
            completeExceptionally(throwable);
         } else {
            complete(null);
         }
      }
   }
}
