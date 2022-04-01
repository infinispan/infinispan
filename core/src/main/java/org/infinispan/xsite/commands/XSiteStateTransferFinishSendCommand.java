package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * Finish sending XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferFinishSendCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 108;

   private String siteName;
   private boolean statusOk;

   // For CommandIdUniquenessTest only
   public XSiteStateTransferFinishSendCommand() {
      super(null);
   }

   public XSiteStateTransferFinishSendCommand(ByteString cacheName) {
      this(cacheName, null, false);
   }

   public XSiteStateTransferFinishSendCommand(ByteString cacheName, String siteName, boolean statusOk) {
      super(cacheName);
      this.siteName = siteName;
      this.statusOk = statusOk;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      XSiteStateTransferManager stateTransferManager = registry.getXSiteStateTransferManager().running();
      stateTransferManager.notifyStatePushFinished(siteName, getOrigin(), statusOk);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeUTF(siteName);
      output.writeBoolean(statusOk);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = input.readUTF();
      statusOk = input.readBoolean();
   }

   @Override
   public String toString() {
      return "XSiteStateTransferFinishSendCommand{" +
            "siteName='" + siteName + '\'' +
            ", statusOk=" + statusOk +
            ", cacheName=" + cacheName +
            '}';
   }
}
