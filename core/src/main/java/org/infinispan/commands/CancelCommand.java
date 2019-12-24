package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
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

   private CancelCommand() {
      super(null);
   }

   public CancelCommand(ByteString ownerCacheName) {
      super(ownerCacheName);
   }

   public CancelCommand(ByteString ownerCacheName, UUID commandToCancel) {
      super(ownerCacheName);
      this.commandToCancel = commandToCancel;
   }

   @Override
   public CompletableFuture<Object> invokeAsync(ComponentRegistry registry) throws Throwable {
      log.trace("Cancelling " + commandToCancel);
      registry.getCancellationService().running().cancel(commandToCancel);
      log.trace("Cancelled " + commandToCancel);
      return CompletableFutures.completedNull();
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
