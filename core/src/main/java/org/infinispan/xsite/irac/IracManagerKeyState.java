package org.infinispan.xsite.irac;

import java.util.Collection;

import org.infinispan.xsite.XSiteBackup;

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
    * This method marks the state to complete.
    * <p>
    * It returns {@code true} if the state is updated and {@code false} if it isn't (for example, a concurrent {@link
    * #discard()} invocation may happen).
    *
    * @return {@code true} if the class state is updated.
    */
   boolean done();

   /**
    * Discards this state.
    */
   void discard();

   /**
    * Mark the given site as successfully received the current state.
    *
    * @param site: Site that received the state.
    */
   void successFor(XSiteBackup site);

   /**
    * Check if given site received this state.
    *
    * @param site: Site to verify.
    * @return true if successfully sent, false otherwise.
    */
   boolean wasSuccessful(XSiteBackup site);

   /**
    * Check if all given sites have received this state.
    */
   boolean successfullySent(Collection<? extends XSiteBackup> sites);
}
