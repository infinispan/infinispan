package org.infinispan.xsite.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;

/**
 * Start send XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferStartSendCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 104;

   private String siteName;
   private int topologyId;

   // For CommandIdUniquenessTest only
   public XSiteStateTransferStartSendCommand() {
      super(null);
   }

   public XSiteStateTransferStartSendCommand(ByteString cacheName) {
      this(cacheName, null, -1);
   }

   public XSiteStateTransferStartSendCommand(ByteString cacheName, String siteName, int topologyId) {
      super(cacheName);
      this.siteName = siteName;
      this.topologyId = topologyId;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) {
      invokeLocal(registry.getXSiteStateTransferManager().running().getStateProvider());
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
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = input.readUTF();
      topologyId = input.readInt();
   }

   @Override
   public String toString() {
      return "XSiteStateTransferStartSendCommand{" +
            "siteName='" + siteName + '\'' +
            ", topologyId=" + topologyId +
            ", cacheName=" + cacheName +
            '}';
   }

   public void invokeLocal(XSiteStateProvider provider) {
      provider.startStateTransfer(siteName, getOrigin(), topologyId);
   }
}
