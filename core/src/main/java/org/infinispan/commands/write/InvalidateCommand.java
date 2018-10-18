package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;


/**
 * Removes an entry from memory.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class InvalidateCommand extends AbstractTopologyAffectedCommand implements WriteCommand, RemoteLockCommand {
   public static final int COMMAND_ID = 6;
   protected Object[] keys;
   protected CommandInvocationId commandInvocationId;

   public InvalidateCommand() {
   }

   public InvalidateCommand(long flagsBitSet, CommandInvocationId commandInvocationId, Object... keys) {
      this.keys = keys;
      this.commandInvocationId = commandInvocationId;
      setFlagsBitSet(flagsBitSet);
   }

   public InvalidateCommand(long flagsBitSet, Collection<Object> keys, CommandInvocationId commandInvocationId) {
      this(flagsBitSet, commandInvocationId, keys == null || keys.isEmpty() ? Util.EMPTY_OBJECT_ARRAY : keys.toArray(new Object[keys.size()]));
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
   public boolean canBlock() {
      return false;
   }

   @Override
   public String toString() {
      return "InvalidateCommand{keys=" +
            toStr(Arrays.asList(keys)) +
            '}';
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      CommandInvocationId.writeTo(output, commandInvocationId);
      MarshallUtil.marshallArray(keys, output);
      output.writeLong(getFlagsBitSet());
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandInvocationId = CommandInvocationId.readFrom(input);
      keys = MarshallUtil.unmarshallArray(input, Util::objectArray);
      setFlagsBitSet(input.readLong());
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
      return CollectionFactory.makeSet(keys);
   }

   @Override
   public void fail() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
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
      // TODO Return LoadType.OWNER only if there are invalidation listeners registered
      return LoadType.OWNER;
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
}
