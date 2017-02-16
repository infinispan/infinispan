package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A command that represents an acknowledge sent by a backup owner to the originator.
 * <p>
 * The acknowledge signals a successful execution of a multi-key command, like {@link PutMapCommand}. It contains the
 * segments ids of the updated keys.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupMultiKeyAckCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = 41;
   private CommandInvocationId commandInvocationId;
   private CommandAckCollector commandAckCollector;
   private int segment;
   private int topologyId;

   public BackupMultiKeyAckCommand() {
      super(null);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int segment,
         int topologyId) {
      super(cacheName);
      this.commandInvocationId = commandInvocationId;
      this.segment = segment;
      this.topologyId = topologyId;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      commandAckCollector.multiKeyBackupAck(commandInvocationId, getOrigin(), segment, topologyId);
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
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeInt(segment);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      segment = input.readInt();
      topologyId = input.readInt();
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "BackupMultiKeyAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", segment=" + segment +
            ", topologyId=" + topologyId +
            '}';
   }
}
