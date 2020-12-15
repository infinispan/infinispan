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
 * Cancel sending XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class XSiteStateTransferCancelSendCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 105;

   private String siteName;

   // For CommandIdUniquenessTest only
   public XSiteStateTransferCancelSendCommand() {
      super(null);
   }

   public XSiteStateTransferCancelSendCommand(ByteString cacheName) {
      this(cacheName, null);
   }

   public XSiteStateTransferCancelSendCommand(ByteString cacheName, String siteName) {
      super(cacheName);
      this.siteName = siteName;
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
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      siteName = input.readUTF();
   }

   @Override
   public String toString() {
      return "XSiteStateTransferCancelSendCommand{" +
            "siteName='" + siteName + '\'' +
            ", cacheName=" + cacheName +
            '}';
   }

   public void invokeLocal(XSiteStateProvider provider) {
      provider.cancelStateTransfer(siteName);
   }
}
