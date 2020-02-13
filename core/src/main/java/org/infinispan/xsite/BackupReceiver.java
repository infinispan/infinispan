package org.infinispan.xsite;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;

/**
 * Component present on a backup site that manages the backup information and logic.
 *
 * @see ClusteredCacheBackupReceiver
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupReceiver {

   Cache getCache();

   CompletionStage<Void> handleRemoteCommand(VisitableCommand command, boolean preserveOrder);

   /**
    * It handles starting the state transfer from a remote site. The command must be broadcast to the entire cluster in
    * which the cache exists.
    */
   CompletionStage<Void> handleStartReceivingStateTransfer(XSiteStateTransferStartReceiveCommand command);

   /**
    * It handles finishing the state transfer from a remote site. The command must be broadcast to the entire cluster in
    * which the cache exists.
    */
   CompletionStage<Void> handleEndReceivingStateTransfer(XSiteStateTransferFinishReceiveCommand command);

   /**
    * It handles the state transfer state from a remote site. It is possible to have a single node applying the state or
    * forward the state to respective primary owners.
    */
   CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd);
}
