package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

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
public class BackupMultiKeyAckCommand extends BackupAckCommand {

   public static final byte COMMAND_ID = 41;
   private int segment;

   public BackupMultiKeyAckCommand() {
      super(null);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   public BackupMultiKeyAckCommand(ByteString cacheName, long id, int segment,
         int topologyId) {
      super(cacheName, id, topologyId);
      this.segment = segment;
   }

   @Override
   public void ack(CommandAckCollector ackCollector) {
      ackCollector.multiKeyBackupAck(id, getOrigin(), segment, topologyId);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
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

   @Override
   public String toString() {
      return "BackupMultiKeyAckCommand{" +
            "id=" + id +
            ", segment=" + segment +
            ", topologyId=" + topologyId +
            '}';
   }
}
