package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.ReplicableCommand;

/**
 * A command that represents an acknowledge sent by a backup owner to the originator.
 * <p>
 * The acknowledge signals a successful execution of the operation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class BackupAckCommand implements ReplicableCommand {

   public static final byte COMMAND_ID = 2;
   private long id;
   private int topologyId;

   public BackupAckCommand() {
   }

   public BackupAckCommand(long id, int topologyId) {
      this.id = id;
      this.topologyId = topologyId;
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(id);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      id = input.readLong();
      topologyId = input.readInt();
   }

   public long getId() {
      return id;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "BackupAckCommand{" +
            "id=" + id +
            ", topologyId=" + topologyId +
            '}';
   }
}
