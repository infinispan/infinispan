package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;

/**
 * Component present on a backup site that manages the backup information and logic.
 *
 * @see BackupReceiverImpl
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupReceiver {

   Cache getCache();

   Object handleRemoteCommand(VisitableCommand command) throws Throwable;
}
