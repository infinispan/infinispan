package org.infinispan.commands.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserObjectOutput;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * Same as {@link PrepareCommand} except that the transaction originator makes evident the versions of entries touched
 * and stored in a transaction context so that accurate write skew checks may be performed by the lock owner(s).
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedPrepareCommand extends PrepareCommand {
   public static final byte COMMAND_ID = 26;
   private EntryVersionsMap versionsSeen = null;

   public VersionedPrepareCommand() {
      super(null);
   }

   public VersionedPrepareCommand(ByteString cacheName, GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      // VersionedPrepareCommands are *always* 2-phase, except when retrying a prepare.
      super(cacheName, gtx, modifications, onePhase);
   }

   public VersionedPrepareCommand(ByteString cacheName) {
      super(cacheName);
   }

   public EntryVersionsMap getVersionsSeen() {
      return versionsSeen;
   }

   public void setVersionsSeen(EntryVersionsMap versionsSeen) {
      this.versionsSeen = versionsSeen;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      super.writeTo(output, entryFactory); //writes global tx, one phase, retried and mods.
      MarshallUtil.marshallMap(versionsSeen, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      versionsSeen = MarshallUtil.unmarshallMap(input, EntryVersionsMap::new);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "VersionedPrepareCommand {" +
            "modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            ", retried=" + retriedCommand +
            ", versionsSeen=" + versionsSeen +
            ", gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}
