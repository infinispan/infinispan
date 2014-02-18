package org.infinispan.xsite.statetransfer;

import org.infinispan.context.InvocationContext;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.Arrays;

/**
 * Wraps the state to be sent to another site
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStatePushCommand extends XSiteReplicateCommand {

   public static final byte COMMAND_ID = 33;
   private XSiteState[] chunk;
   private XSiteStateConsumer consumer;

   public XSiteStatePushCommand(String cacheName, XSiteState[] chunk) {
      super(cacheName);
      this.chunk = chunk;
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
      return chunk;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID) {
         throw new IllegalArgumentException("CommandId is not valid! (" + commandId + " != " + COMMAND_ID + ")");
      }
      this.chunk = Arrays.copyOf(parameters, parameters.length, XSiteState[].class);
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
