package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

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
   private CommandAckCollector commandAckCollector;
   private int segment;
   private long id;
   private int topologyId;

   public BackupMultiKeyAckCommand() {
      super(null);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName, long id, int segment,
         int topologyId) {
      super(cacheName);
      this.id = id;
      this.segment = segment;
      this.topologyId = topologyId;
   }

   public void ack() {
      commandAckCollector.multiKeyBackupAck(id, getOrigin(), segment, topologyId);
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
   public boolean canBlock() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      output.writeLong(id);
      output.writeInt(segment);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readLong();
      segment = input.readInt();
      topologyId = input.readInt();
   }

   public void setCommandAckCollector(CommandAckCollector commandAckCollector) {
      this.commandAckCollector = commandAckCollector;
   }

   @Override
   public String toString() {
      return "BackupMultiKeyAckCommand{" +
            "id=" + id +
            ", segment=" + segment +
            ", topologyId=" + topologyId +
            '}';
   }
}
