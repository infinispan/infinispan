package org.infinispan.commands.write;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
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
@ProtoTypeId(ProtoStreamTypeIds.BACKUP_MULTI_KEY_ACK_COMMAND)
public class BackupMultiKeyAckCommand extends BaseRpcCommand {

   @ProtoField(2)
   final long id;

   @ProtoField(3)
   final int topologyId;

   @ProtoField(4)
   final int segment;

   @ProtoFactory
   public BackupMultiKeyAckCommand(ByteString cacheName, long id, int segment, int topologyId) {
      super(cacheName);
      this.id = id;
      this.topologyId = topologyId;
      this.segment = segment;
   }

   public void ack(CommandAckCollector ackCollector) {
      ackCollector.backupAck(id, getOrigin(), segment, topologyId);
   }

   @Override
   public final boolean isReturnValueExpected() {
      return false;
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
