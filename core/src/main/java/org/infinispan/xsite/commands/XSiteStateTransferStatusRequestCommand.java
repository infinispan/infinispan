package org.infinispan.xsite.commands;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.statetransfer.StateTransferStatus;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * Get XSite state transfer status.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_STATUS_REQUEST_COMMAND)
public class XSiteStateTransferStatusRequestCommand extends BaseRpcCommand {

   @ProtoFactory
   public XSiteStateTransferStatusRequestCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletionStage<Map<String, StateTransferStatus>> invokeAsync(ComponentRegistry registry) {
      XSiteStateTransferManager stateTransferManager = registry.getXSiteStateTransferManager().running();
      return CompletableFuture.completedFuture(stateTransferManager.getStatus());
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferStatusRequestCommand{" +
            "cacheName=" + cacheName +
            '}';
   }
}
