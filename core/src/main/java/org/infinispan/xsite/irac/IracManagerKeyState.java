package org.infinispan.xsite.irac;

/**
 * Keeps a key state for {@link IracManager}.
 * <p>
 * There are 3 major information stored:
 * <p>
 * 1) If it is an expiration update. Expiration update needs special handling since, in case of conflict, it should be
 * dropped.
 * <p>
 * 2) If it is a state transfer request. State transfer happens in batches in the sender and it needs to keep track of
 * that ouside {@link IracManager} scope.
 * <p>
 * 3) Sending status. If the update has been sent to the remote site or not.
 *
 * @since 14
 */
interface IracManagerKeyState extends IracManagerKeyInfo {

   /**
    * @return {@code true} if it is an expiration update.
    */
   boolean isExpiration();

   /**
    * @return {@code true} if it is a state transfer update.
    */
   boolean isStateTransfer();

   /**
    * This method checks if the update can be sent to the remote site.
    * <p>
    * It returns {@link false} when the update is in progress, or it is discarded.
    *
    * @return {@code true} if the key's update needs to be sent to the remote site.
    */
   boolean canSend();

   /**
    * Update this class status to be ready to send.
    */
   void retry();

   /**
    * It returns {@code true} if the state is successfully applied in all online sites.
    *
    * @return {@code true} if the state is successfully applied.
    */
   boolean isDone();

   /**
    * Discards this state.
    */
   void discard();

   /**
    * Mark the given site as successfully received the current state.
    *
    * @param site: Site that received the state.
    */
   void successFor(IracXSiteBackup site);

   /**
    * Check if given site received this state.
    *
    * @param site: Site to verify.
    * @return true if successfully sent, false otherwise.
    */
   boolean wasSuccessful(IracXSiteBackup site);
}
