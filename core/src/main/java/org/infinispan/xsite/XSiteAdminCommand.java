package org.infinispan.xsite;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;

/**
 * Command used for handling XSiteReplication administrative operations.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class XSiteAdminCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 32;

   public enum AdminOperation {
      SITE_STATUS,
      STATUS,
      TAKE_OFFLINE,
      BRING_ONLINE,
      AMEND_TAKE_OFFLINE;

      private static final AdminOperation[] CACHED_VALUES = values();
   }

   public enum Status {
      OFFLINE, ONLINE
   }

   private String siteName;
   private Integer afterFailures;
   private Long minTimeToWait;
   private AdminOperation adminOperation;

   private BackupSender backupSender;

   public XSiteAdminCommand() {
      super(null);// For command id uniqueness test
   }

   public XSiteAdminCommand(String cacheName) {
      super(cacheName);// For command id uniqueness test
   }

   public XSiteAdminCommand(String cacheName, String siteName, AdminOperation op, Integer afterFailures, Long minTimeToWait) {
      this(cacheName);
      this.siteName = siteName;
      this.adminOperation = op;
      this.afterFailures = afterFailures;
      this.minTimeToWait = minTimeToWait;
   }

   public void init(BackupSender backupSender) {
      this.backupSender = backupSender;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      switch (adminOperation) {
         case SITE_STATUS: {
            if (backupSender.getOfflineStatus(siteName).isOffline()) {
               return Status.OFFLINE;
            } else {
               return Status.ONLINE;
            }
         }
         case STATUS: {
            return backupSender.status();
         }
         case TAKE_OFFLINE: {
            return backupSender.takeSiteOffline(siteName);
         }
         case BRING_ONLINE: {
            return backupSender.bringSiteOnline(siteName);
         }
         case AMEND_TAKE_OFFLINE: {
            backupSender.getOfflineStatus(siteName).amend(afterFailures, minTimeToWait);
            return null;
         }
         default: {
            throw new IllegalStateException("Unhandled admin operation " + adminOperation);
         }
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{siteName, afterFailures, minTimeToWait, (byte) adminOperation.ordinal()};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      this.siteName = (String) parameters[0];
      this.afterFailures = (Integer)parameters[1];
      this.minTimeToWait = (Long)parameters[2];
      this.adminOperation = AdminOperation.CACHED_VALUES[(byte)parameters[3]];
   }

   @Override
   public final boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "XSiteAdminCommand{" +
            "siteName='" + siteName + '\'' +
            ", afterFailures=" + afterFailures +
            ", minTimeToWait=" + minTimeToWait +
            ", adminOperation=" + adminOperation +
            ", backupSender=" + backupSender +
            '}';
   }
}
