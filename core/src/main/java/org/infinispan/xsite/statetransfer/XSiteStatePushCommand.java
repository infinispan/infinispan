package org.infinispan.xsite.statetransfer;

import org.infinispan.context.InvocationContext;
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

   public XSiteStatePushCommand(String cacheName, XSiteState[] chunk, long timeoutMillis) {
      super(cacheName);
      this.chunk = chunk;
      this.timeoutMillis = timeoutMillis;
   }

   public XSiteStatePushCommand(String cacheName) {
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
   public Object perform(InvocationContext ctx) throws Throwable {
      consumer.applyState(chunk);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      Object[] result = new Object[chunk.length + 1];
      result[0] = timeoutMillis;
      System.arraycopy(chunk, 0, result, 1, chunk.length);
      return result;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("CommandId is not valid! (" + commandId + " != " + COMMAND_ID + ")");
      }
      this.timeoutMillis = (long) parameters[0];
      this.chunk = new XSiteState[parameters.length - 1];
      //noinspection SuspiciousSystemArraycopy
      System.arraycopy(parameters, 1, chunk, 0, chunk.length);
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
