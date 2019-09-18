package org.infinispan.xsite;

import java.util.Collections;
import java.util.Map;

import javax.transaction.Transaction;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.transaction.impl.AbstractCacheTransaction;

/**
 * A no-op implementation of {@link BackupSender}.
 * <p>
 * This class is used when cross-site replication is disabled.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Scope(value = Scopes.NAMED_CACHE)
public class NoOpBackupSender implements BackupSender {

   private static final NoOpBackupSender INSTANCE = new NoOpBackupSender();

   private NoOpBackupSender() {
   }

   public static NoOpBackupSender getInstance() {
      return INSTANCE;
   }

   @Override
   public BackupResponse backupPrepare(PrepareCommand command, AbstractCacheTransaction cacheTransaction) {
      return BackupSenderImpl.EMPTY_RESPONSE;
   }

   @Override
   public void processResponses(BackupResponse backupResponse, VisitableCommand command) {
      // no-op
   }

   @Override
   public BackupResponse backupWrite(WriteCommand command) {
      return BackupSenderImpl.EMPTY_RESPONSE;
   }

   @Override
   public BackupResponse backupCommit(CommitCommand command) {
      return BackupSenderImpl.EMPTY_RESPONSE;
   }

   @Override
   public BackupResponse backupRollback(RollbackCommand command) {
      return BackupSenderImpl.EMPTY_RESPONSE;
   }

   @Override
   public void processResponses(BackupResponse backupResponse, VisitableCommand command, Transaction transaction) {
      // no-op
   }

   @Override
   public OfflineStatus getOfflineStatus(String siteName) {
      return null;
   }

   @Override
   public Map<String, Boolean> status() {
      return Collections.emptyMap();
   }

   @Override
   public BringSiteOnlineResponse bringSiteOnline(String siteName) {
      return BringSiteOnlineResponse.NO_SUCH_SITE;
   }

   @Override
   public TakeSiteOfflineResponse takeSiteOffline(String siteName) {
      return TakeSiteOfflineResponse.NO_SUCH_SITE;
   }

   @Override
   public String toString() {
      return "NoOpBackupSender{}";
   }
}
