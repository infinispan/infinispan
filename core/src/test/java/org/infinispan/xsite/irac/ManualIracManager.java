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
   private final Map<Object, PendingKeyRequest> pendingKeys;
   @GuardedBy("this")
   private boolean enabled;
   private final List<StateTransferRequest> pendingStateTransfer;

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
         pendingKeys.put(key, new PendingKeyRequest(key, lockOwner, segment, false));
      } else {
         super.trackUpdatedKey(segment, key, lockOwner);
      }
   }

   @Override
   public void trackExpiredKey(int segment, Object key, Object lockOwner) {
      if (enabled) {
         pendingKeys.put(key, new PendingKeyRequest(key, lockOwner, segment, true));
      } else {
         super.trackExpiredKey(segment, key, lockOwner);
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
            .ifPresent(im -> pendingKeys.values().forEach(req -> im.sendStateIfNeeded(origin, segments, req.getSegment(), req.getKey(), req.getLockOwner())));
      super.requestState(origin, segments);
   }

   public synchronized void sendKeys() {
      pendingKeys.values().forEach(this::send);
      pendingKeys.clear();
      pendingStateTransfer.forEach(this::send);
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

   public boolean hasPendingKeys() {
      return !pendingKeys.isEmpty();
   }

   private void send(PendingKeyRequest request) {
      if (request.isExpiration()) {
         super.trackExpiredKey(request.getSegment(), request.getKey(), request.getLockOwner());
         return;
      }
      super.trackUpdatedKey(request.getSegment(), request.getKey(), request.getLockOwner());
   }

   private void send(StateTransferRequest request) {
      CompletionStage<Void> rsp = super.trackForStateTransfer(request.getState());
      rsp.whenComplete(request);
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

      Collection<XSiteState> getState() {
         return state;
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

   private static class PendingKeyRequest {
      private final Object key;
      private final Object lockOwner;
      private final int segment;
      private final boolean expiration;

      private PendingKeyRequest(Object key, Object lockOwner, int segment, boolean expiration) {
         this.key = key;
         this.lockOwner = lockOwner;
         this.segment = segment;
         this.expiration = expiration;
      }

      Object getKey() {
         return key;
      }

      Object getLockOwner() {
         return lockOwner;
      }

      int getSegment() {
         return segment;
      }

      boolean isExpiration() {
         return expiration;
      }
   }
}
