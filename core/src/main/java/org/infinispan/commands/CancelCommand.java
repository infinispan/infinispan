package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command to cancel commands executing in remote VM
 * 
 * @author Vladimir Blagojevic
 * @since 5.2
 */
public class CancelCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(CancelCommand.class);
   public static final byte COMMAND_ID = 34;

   private UUID commandToCancel;
   private CancellationService service;

   private CancelCommand() {
      super(null);
   }

   public CancelCommand(String ownerCacheName) {
      super(ownerCacheName);
   }

   public CancelCommand(String ownerCacheName, UUID commandToCancel) {
      super(ownerCacheName);
      this.commandToCancel = commandToCancel;
   }

   public void init(CancellationService service) {
      this.service = service;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      // grab CancellaltionService and cancel command
      log.trace("Cancelling " + commandToCancel);
      service.cancel(commandToCancel);
      log.trace("Cancelled " + commandToCancel);
      return true;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallUUID(commandToCancel, output, false);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandToCancel = MarshallUtil.unmarshallUUID(input, false);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public int hashCode() {
      int result = 1;
      result = 31 * result + ((commandToCancel == null) ? 0 : commandToCancel.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof CancelCommand)) {
         return false;
      }
      CancelCommand other = (CancelCommand) obj;
      if (commandToCancel == null) {
         if (other.commandToCancel != null) {
            return false;
         }
      } else if (!commandToCancel.equals(other.commandToCancel)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      return "CancelCommand [uuid=" + commandToCancel + "]";
   }

}
