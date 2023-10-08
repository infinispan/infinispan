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
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

/**
 * Finish sending XSite state.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_TRANSFER_FINISH_SEND_COMMAND)
public class XSiteStateTransferFinishSendCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 108;

   @ProtoField(2)
   final String siteName;

   @ProtoField(3)
   final boolean statusOk;

   @ProtoFactory
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
   public String toString() {
      return "XSiteStateTransferFinishSendCommand{" +
            "siteName='" + siteName + '\'' +
            ", statusOk=" + statusOk +
            ", cacheName=" + cacheName +
            '}';
   }
}
