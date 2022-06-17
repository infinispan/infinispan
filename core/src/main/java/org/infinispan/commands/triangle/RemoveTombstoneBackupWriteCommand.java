package org.infinispan.commands.triangle;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.write.RemoveTombstoneCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.ByteString;

/**
 * A {@link RemoveTombstoneCommand} sent from primary owner to backup owner(s).
 *
 * @since 14.0
 */
public class RemoveTombstoneBackupWriteCommand extends BackupWriteCommand {

   public static final int COMMAND_ID = 70;

   private Map<Object, PrivateMetadata> tombstones;

   // for uniqueness test
   @SuppressWarnings("unused")
   public RemoveTombstoneBackupWriteCommand() {
      super(null);
   }

   public RemoveTombstoneBackupWriteCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      writeBase(output);
      MarshallUtil.marshallMap(tombstones, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      readBase(input);
      tombstones = MarshallUtil.unmarshallMap(input, HashMap::new);
   }

   @Override
   WriteCommand createWriteCommand() {
      RemoveTombstoneCommand cmd = new RemoveTombstoneCommand(getCommandInvocationId(), getSegmentId(), getFlags(), tombstones);
      cmd.setTopologyId(getTopologyId());
      return cmd;
   }

   public void setRemoveTombstoneCommand(RemoveTombstoneCommand command) {
      setCommonAttributesFromCommand(command);
      tombstones = command.getTombstones();
   }

   public Map<Object, PrivateMetadata> getTombstones() {
      return tombstones;
   }

   @Override
   public String toString() {
      return "RemoveTombstoneBackupWriteCommand{" +
            "tombstones=" + Util.toStr(tombstones.keySet()) +
            ", segmentId=" + segmentId +
            ", cacheName=" + cacheName +
            '}';
   }
}
