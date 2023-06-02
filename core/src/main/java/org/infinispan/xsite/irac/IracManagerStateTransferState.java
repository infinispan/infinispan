package org.infinispan.xsite.irac;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A {@link IracManagerKeyState} implementation for state transfer requests,
 *
 * @since 14
 */
class IracManagerStateTransferState extends IracManagerKeyChangedState {

   private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

   public IracManagerStateTransferState(int segment, Object key, int numberOfBackups) {
      super(segment, key, "state-transfer", false, numberOfBackups);
   }

   @Override
   public boolean isStateTransfer() {
      return true;
   }

   @Override
   public boolean isDone() {
      if (super.isDone()) {
         completableFuture.complete(null);
         return true;
      }
      return false;
   }

   @Override
   public void discard() {
      super.discard();
      completableFuture.complete(null);
   }

   CompletionStage<Void> getCompletionStage() {
      return completableFuture;
   }
}
