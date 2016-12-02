package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.CompletableFutures;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

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
   private Collection<Integer> segments;
   private int topologyId;

   public BackupMultiKeyAckCommand() {
      super(null);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName, CommandInvocationId commandInvocationId,
         Collection<Integer> segments, int topologyId) {
      super(cacheName);
      this.commandInvocationId = commandInvocationId;
      this.segments = segments;
      this.topologyId = topologyId;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      commandAckCollector.multiKeyBackupAck(commandInvocationId, getOrigin(), segments, topologyId);
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
      MarshallUtil.marshallIntCollection(segments, output);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      segments = MarshallUtil.unmarshallIntCollection(input, ArrayList::new);
      topologyId = input.readInt();
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "BackupMultiKeyAckCommand{" +
            "commandInvocationId=" + commandInvocationId +
            ", segments=" + segments +
            ", topologyId=" + topologyId +
            '}';
   }
}
