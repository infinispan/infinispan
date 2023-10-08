package org.infinispan.commands.tx;

import java.util.List;
import java.util.Map;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * Same as {@link PrepareCommand} except that the transaction originator makes evident the versions of entries touched
 * and stored in a transaction context so that accurate write skew checks may be performed by the lock owner(s).
 *
 * @author Manik Surtani
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.VERSIONED_PREPARE_COMMAND)
public class VersionedPrepareCommand extends PrepareCommand {
   public static final byte COMMAND_ID = 26;
   private Map<Object, IncrementableEntryVersion> versionsSeen;

   public VersionedPrepareCommand(ByteString cacheName, GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      // VersionedPrepareCommands are *always* 2-phase, except when retrying a prepare.
      super(cacheName, gtx, modifications, onePhase);
   }

   @ProtoFactory
   VersionedPrepareCommand(int topologyId, ByteString cacheName, GlobalTransaction globalTransaction, MarshallableCollection<WriteCommand> wrappedModifications,
                           boolean onePhaseCommit, boolean retriedCommand, MarshallableMap<Object, IncrementableEntryVersion> wrappedVersionsSeen) {
      super(topologyId, cacheName, globalTransaction, wrappedModifications, onePhaseCommit, retriedCommand);
      this.versionsSeen = MarshallableMap.unwrap(wrappedVersionsSeen);
   }

   @ProtoField(number = 7, name = "versionsSeen")
   MarshallableMap<Object, IncrementableEntryVersion> getWrappedVersionsSeen() {
      return MarshallableMap.create(versionsSeen);
   }

   public Map<Object, IncrementableEntryVersion> getVersionsSeen() {
      return versionsSeen;
   }

   public void setVersionsSeen(Map<Object, IncrementableEntryVersion> versionsSeen) {
      this.versionsSeen = versionsSeen;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public String toString() {
      return "VersionedPrepareCommand {" +
            "modifications=" + modifications +
            ", onePhaseCommit=" + onePhaseCommit +
            ", retried=" + retriedCommand +
            ", versionsSeen=" + versionsSeen +
            ", gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}
