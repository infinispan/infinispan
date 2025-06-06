package org.infinispan.xsite.statetransfer;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE_PUSH_COMMAND)
public class XSiteStatePushCommand extends BaseRpcCommand {

   private final List<XSiteState> chunk;

   @ProtoFactory
   public XSiteStatePushCommand(ByteString cacheName, List<XSiteState> chunk) {
      super(cacheName);
      this.chunk = chunk;
   }

   @ProtoField(2)
   public List<XSiteState> getChunk() {
      return chunk;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      BlockingManager bm = componentRegistry.getGlobalComponentRegistry().getComponent(BlockingManager.class);
      return bm.runBlocking(() -> {
         XSiteStateConsumer stateConsumer = componentRegistry.getXSiteStateTransferManager().running().getStateConsumer();
         stateConsumer.applyState(chunk);
      }, "xsite-state-push");
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "XSiteStatePushCommand{" +
            "cacheName=" + cacheName +
            " (" + chunk.size() + " keys)" +
            '}';
   }
}
