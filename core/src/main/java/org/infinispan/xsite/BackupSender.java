package org.infinispan.xsite;

import jakarta.transaction.Transaction;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.transaction.impl.AbstractCacheTransaction;

/**
 * Component responsible with sending backup data to remote sites. The send operation is executed async, it's up to the
 * caller to wait on the returned {@link BackupResponse} in the case it wants an sync call.
 *
 * @see BackupResponse
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupSender {

   /**
    * Prepares a transaction on the remote site.
    */
   InvocationStage backupPrepare(PrepareCommand command, AbstractCacheTransaction cacheTransaction, Transaction transaction);

   InvocationStage backupCommit(CommitCommand command, Transaction transaction);

   InvocationStage backupRollback(RollbackCommand command, Transaction transaction);

   InvocationStage backupWrite(WriteCommand command, WriteCommand originalCommand);

   InvocationStage backupClear(ClearCommand command);

}
