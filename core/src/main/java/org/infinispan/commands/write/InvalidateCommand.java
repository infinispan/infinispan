package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;


/**
 * Removes an entry from memory.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.INVALIDATE_COMMAND)
public class InvalidateCommand extends AbstractTopologyAffectedCommand implements WriteCommand, RemoteLockCommand {
   public static final int COMMAND_ID = 6;
   protected Object[] keys;
   protected CommandInvocationId commandInvocationId;

   public InvalidateCommand(long flagsBitSet, CommandInvocationId commandInvocationId, Object... keys) {
      super(flagsBitSet, -1);
      this.keys = keys;
      this.commandInvocationId = commandInvocationId;
   }

   public InvalidateCommand(long flagsBitSet, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      this(flagsBitSet, commandInvocationId, keys == null || keys.isEmpty() ? Util.EMPTY_OBJECT_ARRAY : keys.toArray());
   }

   @ProtoFactory
   protected InvalidateCommand(long flagsWithoutRemote, int topologyId, CommandInvocationId commandInvocationId,
                     MarshallableArray<Object> wrappedKeys) {
      super(flagsWithoutRemote, topologyId);
      this.keys = MarshallableArray.unwrap(wrappedKeys, new Object[0]);
      this.commandInvocationId = commandInvocationId;
   }

   @ProtoField(number = 3, name = "keys")
   public MarshallableArray<Object> getWrappedKeys() {
      return MarshallableArray.create(keys);
   }

   @Override
   @ProtoField(number = 4)
   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
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
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitInvalidateCommand(ctx, this);
   }

   public Object[] getKeys() {
      return keys;
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return new HashSet<>(Arrays.asList(keys));
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public PrivateMetadata getInternalMetadata(Object key) {
      //TODO? support invalidation?
      return null;
   }

   @Override
   public void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      //no-op
   }

   @Override
   public Collection<?> getKeysToLock() {
      return Arrays.asList(keys);
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

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      InvalidateCommand that = (InvalidateCommand) obj;
      if (!hasSameFlags(that))
         return false;
      return Arrays.equals(keys, that.keys);
   }

   @Override
   public int hashCode() {
      return keys != null ? Arrays.hashCode(keys) : 0;
   }

   @Override
   public String toString() {
      return "InvalidateCommand{keys=" +
            toStr(Arrays.asList(keys)) +
            '}';
   }
}
