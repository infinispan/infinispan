package org.infinispan.commands.tx;

import java.io.IOException;
import java.io.ObjectInput;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * The same as a {@link CommitCommand} except that version information is also carried by this command, used by
 * optimistically transactional caches making use of write skew checking when using {@link org.infinispan.util.concurrent.IsolationLevel#REPEATABLE_READ}.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedCommitCommand extends CommitCommand {
   public static final byte COMMAND_ID = 27;
   private EntryVersionsMap updatedVersions;

   public VersionedCommitCommand() {
      super(null);
   }

   public VersionedCommitCommand(ByteString cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public VersionedCommitCommand(ByteString cacheName) {
      super(cacheName);
   }

   public EntryVersionsMap getUpdatedVersions() {
      return updatedVersions;
   }

   public void setUpdatedVersions(EntryVersionsMap updatedVersions) {
      this.updatedVersions = updatedVersions;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      super.writeTo(output, entryFactory); //write global tx
      MarshallUtil.marshallMap(updatedVersions, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      updatedVersions = MarshallUtil.unmarshallMap(input, EntryVersionsMap::new);
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
