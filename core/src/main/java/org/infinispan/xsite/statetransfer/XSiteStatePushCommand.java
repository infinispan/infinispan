package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStatePushCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 33;
   private XSiteState[] chunk;

   public XSiteStatePushCommand(ByteString cacheName, XSiteState[] chunk) {
      super(cacheName);
      this.chunk = chunk;
   }

   public XSiteStatePushCommand(ByteString cacheName) {
      super(cacheName);
   }

   public XSiteStatePushCommand() {
      this(null);
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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallArray(chunk, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      chunk = MarshallUtil.unmarshallArray(input, XSiteState[]::new);
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
