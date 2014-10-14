package org.infinispan.xsite.statetransfer;

import org.infinispan.context.InvocationContext;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Command used to control the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferControlCommand extends XSiteReplicateCommand {

   public static final int COMMAND_ID = 28;

   private StateTransferControl control;
   private XSiteStateProvider provider;
   private XSiteStateConsumer consumer;
   private XSiteStateTransferManager stateTransferManager;
   private String siteName;
   private boolean statusOk;
   private int topologyId;

   public XSiteStateTransferControlCommand(String cacheName, StateTransferControl control, String siteName) {
      super(cacheName);
      this.control = control;
      this.siteName = siteName;
   }

   public XSiteStateTransferControlCommand(String cacheName) {
      super(cacheName);
   }

   public XSiteStateTransferControlCommand() {
      super(null);
   }

   @Override
   public Object performInLocalSite(BackupReceiver receiver) throws Throwable {
      receiver.handleStateTransferControl(this);
      return null;
   }

   public final void initialize(XSiteStateProvider provider, XSiteStateConsumer consumer,
                                XSiteStateTransferManager stateTransferManager) {
      this.provider = provider;
      this.consumer = consumer;
      this.stateTransferManager = stateTransferManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      switch (control) {
         case START_SEND:
            provider.startStateTransfer(siteName, getOrigin(), topologyId);
            break;
         case START_RECEIVE:
            consumer.startStateTransfer(siteName);
            break;
         case FINISH_RECEIVE:
            consumer.endStateTransfer(siteName);
            break;
         case FINISH_SEND:
            stateTransferManager.notifyStatePushFinished(siteName, getOrigin(), statusOk);
            break;
         case CANCEL_SEND:
            provider.cancelStateTransfer(siteName);
            break;
         case RESTART_SEND:
            provider.cancelStateTransfer(siteName);
            provider.startStateTransfer(siteName, getOrigin(), topologyId);
            break;
         case STATUS_REQUEST:
            return stateTransferManager.getStatus();
         case CLEAR_STATUS:
            stateTransferManager.clearStatus();
            break;
         default:
            throw new IllegalStateException("Unknown control command: " + consumer);
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{control, siteName, statusOk, topologyId};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("CommandId is not valid! (" + commandId + " != " + COMMAND_ID + ")");
      }
      this.control = (StateTransferControl) parameters[0];
      this.siteName = (String) parameters[1];
      this.statusOk = (Boolean) parameters[2];
      this.topologyId = (Integer) parameters[3];
   }

   @Override
   public boolean isReturnValueExpected() {
      return this.control == StateTransferControl.STATUS_REQUEST;
   }

   public void setStatusOk(boolean statusOk) {
      this.statusOk = statusOk;
   }

   public void setSiteName(String siteName) {
      this.siteName = siteName;
   }

   public String getSiteName() {
      return siteName;
   }

   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   public XSiteStateTransferControlCommand copyForCache(String cacheName) {
      //cache name is final. we need to copy the command.
      XSiteStateTransferControlCommand copy = new XSiteStateTransferControlCommand(cacheName);
      copy.control = this.control;
      copy.provider = this.provider;
      copy.consumer = this.consumer;
      copy. stateTransferManager = this.stateTransferManager;
      copy.siteName = this.siteName;
      copy.statusOk = this.statusOk;
      copy.topologyId = this.topologyId;
      copy.setOriginSite(this.getOriginSite());
      copy.setOrigin(this.getOrigin());
      return copy;
   }

   public static enum StateTransferControl {
      START_SEND,
      START_RECEIVE,
      FINISH_SEND,
      FINISH_RECEIVE,
      CANCEL_SEND,
      RESTART_SEND,
      STATUS_REQUEST,
      CLEAR_STATUS
   }

   @Override
   public String toString() {
      return "XSiteStateTransferControlCommand{" +
            "control=" + control +
            ", siteName='" + siteName + '\'' +
            ", statusOk=" + statusOk +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}
