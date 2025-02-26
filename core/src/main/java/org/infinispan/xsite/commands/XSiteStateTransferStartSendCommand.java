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
 * Start send XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_START_SEND_COMMAND)
public class XSiteStateTransferStartSendCommand extends BaseRpcCommand {

   @ProtoField(2)
   final String siteName;

   @ProtoField(3)
   final int topologyId;

   @ProtoFactory
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
   public boolean isReturnValueExpected() {
      return false;
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
