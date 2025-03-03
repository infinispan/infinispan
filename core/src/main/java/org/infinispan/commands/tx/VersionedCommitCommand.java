package org.infinispan.commands.tx;

import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * The same as a {@link CommitCommand} except that version information is also carried by this command, used by
 * optimistically transactional caches making use of write skew checking when using {@link IsolationLevel#REPEATABLE_READ}.
 *
 * @author Manik Surtani
 * @since 5.1
 */
@ProtoTypeId(ProtoStreamTypeIds.VERSIONED_COMMIT_COMMAND)
public class VersionedCommitCommand extends CommitCommand {

   @ProtoField(5)
   MarshallableMap<Object, IncrementableEntryVersion> updatedVersions;

   @ProtoFactory
   VersionedCommitCommand(int topologyId, ByteString cacheName, GlobalTransaction globalTransaction,
                          Map<Integer, IracMetadata> iracMetadataMap,
                          MarshallableMap<Object, IncrementableEntryVersion> updatedVersions) {
      super(topologyId, cacheName, globalTransaction, iracMetadataMap);
      this.updatedVersions = updatedVersions;
   }

   public VersionedCommitCommand(ByteString cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public Map<Object, IncrementableEntryVersion> getUpdatedVersions() {
      return MarshallableMap.unwrap(updatedVersions);
   }

   public void setUpdatedVersions(Map<Object, IncrementableEntryVersion> updatedVersions) {
      this.updatedVersions = MarshallableMap.create(updatedVersions);
   }

   @Override
   public String toString() {
      return "VersionedCommitCommand{gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            ", topologyId=" + getTopologyId() +
            ", updatedVersions=" + updatedVersions +
            '}';
   }
}
