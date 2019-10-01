package org.infinispan.xsite;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

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
    * It handles the state transfer control from a remote site. The control command must be broadcast to the entire
    * cluster in which the cache exists.
    */
   CompletionStage<Void> handleStateTransferControl(XSiteStateTransferControlCommand command);

   /**
    * It handles the state transfer state from a remote site. It is possible to have a single node applying the state or
    * forward the state to respective primary owners.
    */
   CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd);
}
