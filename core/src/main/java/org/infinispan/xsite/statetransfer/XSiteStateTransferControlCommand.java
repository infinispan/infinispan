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
            provider.startStateTransfer(siteName, getOrigin());
            break;
         case START_RECEIVE:
            consumer.startStateTransfer();
            break;
         case FINISH_RECEIVE:
            consumer.endStateTransfer();
            break;
         case FINISH_SEND:
            stateTransferManager.notifyStatePushFinished(siteName, getOrigin());
            break;
         case CANCEL_SEND:
            provider.cancelStateTransfer(siteName);
            break;
      }
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{control, siteName};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("CommandId is not valid! (" + commandId + " != " + COMMAND_ID + ")");
      }
      this.control = (StateTransferControl) parameters[0];
      this.siteName = (String) parameters[1];

   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   public static enum StateTransferControl {
      START_SEND,
      START_RECEIVE,
      FINISH_SEND,
      FINISH_RECEIVE,
      CANCEL_SEND
   }
}
