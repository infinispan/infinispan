package org.infinispan.xsite.irac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * A manually trigger {@link IracManager} delegator.
 * <p>
 * The keys are only sent to the remote site if triggered.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ManualIracManager extends ControlledIracManager {

   private final Map<Object, PendingKeyRequest> pendingKeys = new ConcurrentHashMap<>(16);
   private volatile boolean enabled;
   private final List<StateTransferRequest> pendingStateTransfer = new ArrayList<>(2);

   private ManualIracManager(IracManager actual) {
      super(actual);
   }

   public static ManualIracManager wrapCache(Cache<?, ?> cache) {
      IracManager iracManager = TestingUtil.extractComponent(cache, IracManager.class);
      if (iracManager instanceof ManualIracManager) {
         return (ManualIracManager) iracManager;
      }
      return TestingUtil.wrapComponent(cache, IracManager.class, ManualIracManager::new);
   }

   @Override
   public void trackUpdatedKey(int segment, Object key, Object lockOwner) {
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
   public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      if (enabled) {
         StateTransferRequest request = new StateTransferRequest(stateList);
         pendingStateTransfer.add(request);
         return request;
      } else {
         return super.trackForStateTransfer(stateList);
      }
   }

   @Override
   public void requestState(Address requestor, IntSet segments) {
      //send the state for the keys we have pending in this instance!
      asDefaultIracManager().ifPresent(im -> im.transferStateTo(requestor, segments, pendingKeys.values()));
      super.requestState(requestor, segments);
   }

   @Override
   public boolean containsKey(Object key) {
      return pendingKeys.containsKey(key) ||
            super.containsKey(key) ||
            pendingStateTransfer.stream()
                  .map(StateTransferRequest::getState)
                  .flatMap(Collection::stream)
                  .map(XSiteState::key)
                  .anyMatch(key::equals);
   }

   public void sendKeys() {
      pendingKeys.values().forEach(this::send);
      pendingKeys.clear();
      pendingStateTransfer.forEach(this::send);
      pendingStateTransfer.clear();
   }

   public void enable() {
      enabled = true;
   }

   public void disable(DisableMode disableMode) {
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

   boolean hasPendingKeys() {
      return !pendingKeys.isEmpty();
   }

   private void send(PendingKeyRequest request) {
      if (request.isExpiration()) {
         super.trackExpiredKey(request.getSegment(), request.getKey(), request.getOwner());
         return;
      }
      super.trackUpdatedKey(request.getSegment(), request.getKey(), request.getOwner());
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

   private static class PendingKeyRequest implements IracManagerKeyState {

      private final IracManagerKeyInfo keyInfo;
      private final boolean expiration;

      private PendingKeyRequest(Object key, Object lockOwner, int segment, boolean expiration) {
         keyInfo = new IracManagerKeyInfo(segment, key, lockOwner);
         this.expiration = expiration;
      }

      @Override
      public IracManagerKeyInfo getKeyInfo() {
         return keyInfo;
      }

      @Override
      public Object getKey() {
         return keyInfo.getKey();
      }

      @Override
      public Object getOwner() {
         return keyInfo.getOwner();
      }

      @Override
      public int getSegment() {
         return keyInfo.getSegment();
      }

      @Override
      public boolean isExpiration() {
         return expiration;
      }

      @Override
      public boolean isStateTransfer() {
         return false;
      }

      @Override
      public boolean canSend() {
         return false;
      }

      @Override
      public void retry() {

      }

      @Override
      public boolean isDone() {
         return false;
      }

      @Override
      public void discard() {

      }

      @Override
      public void successFor(IracXSiteBackup site) {
      }

      @Override
      public boolean wasSuccessful(IracXSiteBackup site) {
         return false;
      }
   }
}
