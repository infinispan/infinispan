package org.infinispan.xsite.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserAwareObjectOutput;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
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

   public XSiteStateTransferControlCommand(ByteString cacheName, StateTransferControl control, String siteName) {
      super(cacheName);
      this.control = control;
      this.siteName = siteName;
   }

   public XSiteStateTransferControlCommand(ByteString cacheName) {
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
   public CompletableFuture<Object> invokeAsync() throws Throwable {
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
            return CompletableFuture.completedFuture(stateTransferManager.getStatus());
         case CLEAR_STATUS:
            stateTransferManager.clearStatus();
            break;
         default:
            throw new IllegalStateException("Unknown control command: " + control);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserAwareObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      MarshallUtil.marshallEnum(control, output);
      switch (control) {
         case START_SEND:
         case RESTART_SEND:
            output.writeUTF(siteName);
            output.writeInt(topologyId);
            return;
         case CANCEL_SEND:
            output.writeUTF(siteName);
            return;
         case FINISH_SEND:
            output.writeUTF(siteName);
            output.writeBoolean(statusOk);
            return;
         case START_RECEIVE:
         case FINISH_RECEIVE:
            MarshallUtil.marshallString(siteName, output);
            return;
         case STATUS_REQUEST:
         case CLEAR_STATUS:
            return;
         default:
            throw new IllegalStateException("Unknown control command: " + control);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      control = Objects.requireNonNull(MarshallUtil.unmarshallEnum(input, StateTransferControl::valueOf));
      switch (control) {
         case START_SEND:
         case RESTART_SEND:
            siteName = input.readUTF();
            topologyId = input.readInt();
            return;
         case CANCEL_SEND:
            siteName = input.readUTF();
            return;
         case FINISH_SEND:
            siteName = input.readUTF();
            statusOk = input.readBoolean();
            return;
         case START_RECEIVE:
         case FINISH_RECEIVE:
            siteName = MarshallUtil.unmarshallString(input);
            return;
         case STATUS_REQUEST:
         case CLEAR_STATUS:
            return;
         default:
            throw new IllegalStateException("Unknown control command: " + control);
      }
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

   public XSiteStateTransferControlCommand copyForCache(ByteString cacheName) {
      //cache name is final. we need to copy the command.
      XSiteStateTransferControlCommand copy = new XSiteStateTransferControlCommand(cacheName);
      copy.control = this.control;
      copy.provider = this.provider;
      copy.consumer = this.consumer;
      copy.stateTransferManager = this.stateTransferManager;
      copy.siteName = this.siteName;
      copy.statusOk = this.statusOk;
      copy.topologyId = this.topologyId;
      copy.setOriginSite(this.getOriginSite());
      copy.setOrigin(this.getOrigin());
      return copy;
   }

   public enum StateTransferControl {
      START_SEND,
      START_RECEIVE,
      FINISH_SEND,
      FINISH_RECEIVE,
      CANCEL_SEND,
      RESTART_SEND,
      STATUS_REQUEST,
      CLEAR_STATUS;

      private static final StateTransferControl[] CACHED_VALUES = values();

      private static StateTransferControl valueOf(int index) {
         return CACHED_VALUES[index];
      }
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
