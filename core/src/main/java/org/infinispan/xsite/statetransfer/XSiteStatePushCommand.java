package org.infinispan.xsite.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStatePushCommand extends XSiteReplicateCommand<Void> {

   public static final byte COMMAND_ID = 33;
   private XSiteState[] chunk;
   private long timeoutMillis;

   public XSiteStatePushCommand(ByteString cacheName, XSiteState[] chunk, long timeoutMillis) {
      super(COMMAND_ID, cacheName);
      this.chunk = chunk;
      this.timeoutMillis = timeoutMillis;
   }

   public XSiteStatePushCommand(ByteString cacheName) {
      super(COMMAND_ID, cacheName);
   }

   @Override
   public CompletionStage<Void> performInLocalSite(BackupReceiver receiver, boolean preserveOrder) {
      assert !preserveOrder;
      return receiver.handleStateTransferState(this);
   }

   public XSiteStatePushCommand() {
      this(null);
   }

   public XSiteState[] getChunk() {
      return chunk;
   }

   public long getTimeout() {
      return timeoutMillis;
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
      output.writeLong(timeoutMillis);
      MarshallUtil.marshallArray(chunk, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      timeoutMillis = input.readLong();
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
            ", timeout=" + timeoutMillis +
            " (" + chunk.length + " keys)" +
            '}';
   }
}
