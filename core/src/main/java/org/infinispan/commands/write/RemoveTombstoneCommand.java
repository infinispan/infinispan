package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * Removes a key if it is a tombstone.
 *
 * @since 14.0
 */
public class RemoveTombstoneCommand implements WriteCommand, SegmentSpecificCommand, RemoteLockCommand {

   public static final int COMMAND_ID = 15;

   private CommandInvocationId commandInvocationId;
   private int segment;
   private boolean successful = true; // transient
   private long flags;
   private int topologyId = -1;
   private Map<Object, PrivateMetadata> tombstones;

   public RemoveTombstoneCommand() {
   }

   public RemoveTombstoneCommand(CommandInvocationId commandInvocationId, int segment, long flags, int capacity) {
      this(commandInvocationId, segment, flags);
      tombstones = new HashMap<>(capacity);
   }

   public RemoveTombstoneCommand(CommandInvocationId commandInvocationId, int segment, long flags, Map<Object, PrivateMetadata> tombstones) {
      this(commandInvocationId, segment, flags);
      this.tombstones = tombstones;
   }

   private RemoveTombstoneCommand(CommandInvocationId commandInvocationId, int segment, long flags) {
      this.commandInvocationId = commandInvocationId;
      this.segment = segment;
      this.flags = flags;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveTombstone(ctx, this);
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
   public String toString() {
      return "RemoveTombstoneCommand{" +
            "keys=" + Util.toStr(tombstones.keySet()) +
            ", segment=" + segment +
            ", successful=" + successful +
            ", commandInvocationId=" + commandInvocationId +
            '}';
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return true;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      //no-op
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return Collections.unmodifiableCollection(tombstones.keySet());
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }

   @Override
   public PrivateMetadata getInternalMetadata(Object key) {
      return tombstones.get(key);
   }

   @Override
   public void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      tombstones.put(key, internalMetadata);
   }

   public void removeInternalMetadata(Object key) {
      tombstones.remove(key);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
      MarshallUtil.marshallMap(tombstones, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      segment = UnsignedNumeric.readUnsignedInt(input);
      flags = input.readLong();
      tombstones = MarshallUtil.unmarshallMap(input, HashMap::new);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      flags = bitSet;
   }

   @Override
   public int getSegment() {
      return segment;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public Collection<?> getKeysToLock() {
      return getAffectedKeys();
   }

   @Override
   public Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   public boolean isEmpty() {
      return tombstones.isEmpty();
   }

   public Map<Object, PrivateMetadata> getTombstones() {
      return tombstones;
   }
}
