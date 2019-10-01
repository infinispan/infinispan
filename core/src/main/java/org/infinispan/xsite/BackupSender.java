package org.infinispan.xsite;

import java.util.Map;

import javax.transaction.Transaction;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
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
   InvocationStage backupPrepare(PrepareCommand command, AbstractCacheTransaction cacheTransaction, Transaction transaction) throws Exception;

   InvocationStage backupCommit(CommitCommand command, Transaction transaction) throws Exception;

   InvocationStage backupRollback(RollbackCommand command, Transaction transaction) throws Exception;

   InvocationStage backupWrite(WriteCommand command, VisitableCommand originalCommand) throws Exception;

   OfflineStatus getOfflineStatus(String siteName);

   /**
    * Returns a Map having as entries the site names and as value Boolean.TRUE if the site is online and Boolean.FALSE
    * if it is offline.
    */
   Map<String, Boolean> status();

   enum BringSiteOnlineResponse {
      NO_SUCH_SITE,
      ALREADY_ONLINE,
      BROUGHT_ONLINE
   }

   /**
    * Brings a site with the given name back online.
    */
   BringSiteOnlineResponse bringSiteOnline(String siteName);

   enum TakeSiteOfflineResponse {
      NO_SUCH_SITE,
      ALREADY_OFFLINE,
      TAKEN_OFFLINE
   }

   TakeSiteOfflineResponse takeSiteOffline(String siteName);
}
