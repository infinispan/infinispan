package org.infinispan.commands.triangle;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;

/**
 * A command that tell a backup owner to ignore a sequence id after the primary failed to send a regular write command.
 *
 * @author Dan Berindei
 * @since 12.1
 */
@ProtoTypeId(ProtoStreamTypeIds.BACKUP_NOOP_COMMAND)
public class BackupNoopCommand extends BackupWriteCommand {

   @ProtoFactory
   BackupNoopCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                     long flags, long sequence, int segmentId) {
      super(cacheName, commandInvocationId, topologyId, flags, sequence, segmentId);
   }

   public BackupNoopCommand(ByteString cacheName, WriteCommand command, long sequence, int segmentId) {
      super(cacheName, command, sequence, segmentId);
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "BackupNoopCommand{" + toStringFields() + '}';
   }

   @Override
   WriteCommand createWriteCommand() {
      return null;
   }

   @Override
   String toStringFields() {
      return super.toStringFields();
   }
}
