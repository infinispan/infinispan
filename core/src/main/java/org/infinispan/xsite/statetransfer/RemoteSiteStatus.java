package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.Objects;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.xsite.XSiteBackup;

import net.jcip.annotations.GuardedBy;

/**
 * Cross-Site state transfer status & collector
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class RemoteSiteStatus {

   private final XSiteBackup backup;
   private final boolean isSync;
   @GuardedBy("this")
   private StateTransferStatus status;
   @GuardedBy("this")
   private XSiteStateTransferCollector collector;
   @GuardedBy("this")
   private XSiteStateTransferMode stateTransferMode;

   private RemoteSiteStatus(XSiteBackup backup, boolean isSync, XSiteStateTransferMode stateTransferMode) {
      this.backup = Objects.requireNonNull(backup);
      this.isSync = isSync;
      this.stateTransferMode = stateTransferMode;
      this.status = StateTransferStatus.IDLE;
   }

   public XSiteBackup getBackup() {
      return backup;
   }

   public boolean isSync() {
      return isSync;
   }

   public String getSiteName() {
      return backup.getSiteName();
   }

   public synchronized StateTransferStatus getStatus() {
      return status;
   }

   public synchronized void clearStatus() {
      if (collector == null) {
         status = StateTransferStatus.IDLE;
      }
   }

   public synchronized boolean startStateTransfer(Collection<Address> members) {
      if (collector != null) {
         return false;
      }
      collector = new XSiteStateTransferCollector(members);
      status = StateTransferStatus.SENDING;
      return true;
   }

   public synchronized boolean restartStateTransfer(Collection<Address> newMembers) {
      if (collector == null) {
         return false;
      }
      collector = new XSiteStateTransferCollector(newMembers);
      return true;
   }

   public synchronized boolean confirmStateTransfer(Address node, boolean statusOk) {
      if (collector == null) {
         return false;
      }
      if (collector.confirmStateTransfer(node, statusOk)) {
         status = statusOk ? StateTransferStatus.SEND_OK : StateTransferStatus.SEND_FAILED;
         collector = null;
         return true;
      }
      return false;
   }

   public synchronized void cancelStateTransfer() {
      if (collector == null) {
         return;
      }
      collector = null;
      status = StateTransferStatus.SEND_CANCELED;
   }

   public synchronized boolean isStateTransferInProgress() {
      return collector != null;
   }

   public synchronized void failStateTransfer() {
      if (collector == null) {
         return;
      }
      collector = null;
      status = StateTransferStatus.SEND_FAILED;
   }

   public static RemoteSiteStatus fromConfiguration(BackupConfiguration configuration) {
      XSiteBackup backup = new XSiteBackup(configuration.site(), true, configuration.replicationTimeout());
      return new RemoteSiteStatus(backup, configuration.isSyncBackup(), configuration.stateTransfer().mode());
   }

   public XSiteStateTransferMode stateTransferMode() {
      return stateTransferMode;
   }

   public boolean setStateTransferMode(XSiteStateTransferMode mode) {
      if (isSync) {
         return false;
      }
      stateTransferMode = mode;
      return true;
   }
}
