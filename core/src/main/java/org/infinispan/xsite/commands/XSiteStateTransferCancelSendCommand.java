package org.infinispan.xsite.commands;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;

/**
 * Cancel sending XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_CANCEL_SEND_COMMAND)
public class XSiteStateTransferCancelSendCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 105;

   @ProtoField(number = 2)
   final String siteName;

   @ProtoFactory
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
