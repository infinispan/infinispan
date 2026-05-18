package org.infinispan.commands.triangle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.write.RemoveAllCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;

/**
 * A {@link BackupWriteCommand} implementation for {@link RemoveAllCommand}.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOVE_ALL_BACKUP_WRITE_COMMAND)
public class RemoveAllBackupWriteCommand extends BackupWriteCommand {

   private final Collection<Object> keys;
   private final Map<Object, PrivateMetadata> internalMetadataMap;

   public RemoveAllBackupWriteCommand(ByteString cacheName, RemoveAllCommand command, long sequence, int segmentId,
                                      Collection<Object> keys) {
      super(cacheName, command, sequence, segmentId);
      this.keys = new ArrayList<>(keys);
      this.internalMetadataMap = new HashMap<>();
      for (Object key : this.keys) {
         PrivateMetadata pm = command.getInternalMetadata(key);
         if (pm != null) {
            internalMetadataMap.put(key, pm);
         }
      }
   }

   @ProtoFactory
   RemoveAllBackupWriteCommand(ByteString cacheName, CommandInvocationId commandInvocationId, int topologyId,
                               long flags, long sequence, int segmentId,
                               MarshallableCollection<Object> wrappedKeys,
                               MarshallableMap<Object, PrivateMetadata> internalMetadata) {
      super(cacheName, commandInvocationId, topologyId, flags, sequence, segmentId);
      Collection<Object> unwrapped = MarshallableCollection.unwrap(wrappedKeys);
      this.keys = unwrapped != null ? unwrapped : new ArrayList<>();
      this.internalMetadataMap = MarshallableMap.unwrap(internalMetadata);
   }

   @ProtoField(7)
   MarshallableCollection<Object> getWrappedKeys() {
      return MarshallableCollection.create(keys);
   }

   @ProtoField(8)
   MarshallableMap<Object, PrivateMetadata> getInternalMetadata() {
      return MarshallableMap.create(internalMetadataMap);
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "RemoveAllBackupWriteCommand{" + toStringFields() + '}';
   }

   @Override
   WriteCommand createWriteCommand() {
      RemoveAllCommand cmd = new RemoveAllCommand(cacheName, keys, getFlags(), getCommandInvocationId());
      cmd.setForwarded(true);
      internalMetadataMap.forEach(cmd::setInternalMetadata);
      return cmd;
   }

   @Override
   String toStringFields() {
      return super.toStringFields() +
            ", keys=" + keys +
            ", internalMetadata=" + internalMetadataMap;
   }
}
