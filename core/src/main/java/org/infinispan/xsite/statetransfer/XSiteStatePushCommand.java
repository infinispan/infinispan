package org.infinispan.xsite.statetransfer;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_PUSH_COMMAND)
public class XSiteStatePushCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 33;
   private XSiteState[] chunk;

   @ProtoFactory
   public XSiteStatePushCommand(ByteString cacheName, XSiteState[] chunk) {
      super(cacheName);
      this.chunk = chunk;
   }

   @ProtoField(number = 2)
   public XSiteState[] getChunk() {
      return chunk;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      XSiteStateConsumer stateConsumer = componentRegistry.getXSiteStateTransferManager().running().getStateConsumer();
      stateConsumer.applyState(chunk);
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
   public boolean canBlock() {
      return true;
   }

   @Override
   public String toString() {
      return "XSiteStatePushCommand{" +
            "cacheName=" + cacheName +
            " (" + chunk.length + " keys)" +
            '}';
   }
}
