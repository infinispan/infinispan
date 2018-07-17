package org.infinispan.xsite.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserObjectOutput;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStatePushCommand extends XSiteReplicateCommand {

   public static final byte COMMAND_ID = 33;
   private XSiteState[] chunk;
   private long timeoutMillis;
   private XSiteStateConsumer consumer;

   public XSiteStatePushCommand(ByteString cacheName, XSiteState[] chunk, long timeoutMillis) {
      super(cacheName);
      this.chunk = chunk;
      this.timeoutMillis = timeoutMillis;
   }

   public XSiteStatePushCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public Object performInLocalSite(BackupReceiver receiver) throws Throwable {
      receiver.handleStateTransferState(this);
      return null;
   }

   public XSiteStatePushCommand() {
      super(null);
   }

   public void initialize(XSiteStateConsumer consumer) {
      this.consumer = consumer;
   }

   public XSiteState[] getChunk() {
      return chunk;
   }

   public long getTimeout() {
      return timeoutMillis;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      consumer.applyState(chunk);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
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
