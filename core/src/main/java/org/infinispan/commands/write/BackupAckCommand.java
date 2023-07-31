package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;

/**
 * A command that represents an acknowledge sent by a backup owner to the originator.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class BackupAckCommand extends BaseRpcCommand {

   protected long id;
   protected int topologyId;

   protected BackupAckCommand() {
      super(null);
   }

   protected BackupAckCommand(ByteString cacheName) {
      super(cacheName);
   }

   protected BackupAckCommand(ByteString cacheName, long id, int topologyId) {
      super(cacheName);
      this.id = id;
      this.topologyId = topologyId;
   }

   public abstract void ack(CommandAckCollector ackCollector);

   @Override
   public final boolean isReturnValueExpected() {
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

   @Override
   public String toString() {
      return "BackupAckCommand{" +
            "id=" + id +
            ", topologyId=" + topologyId +
            '}';
   }
}
