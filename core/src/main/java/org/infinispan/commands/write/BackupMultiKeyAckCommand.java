package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * A command that represents an acknowledge sent by a backup owner to the originator.
 * <p>
 * The acknowledge signals a successful execution of a backup write command.
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
      ackCollector.backupAck(id, getOrigin(), segment, topologyId);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      super.writeTo(output);
      // todo write vint?
      output.writeInt(segment);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      segment = input.readInt();
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
