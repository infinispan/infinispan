package org.infinispan.commands.write;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * Stuff common to WriteCommands
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractDataWriteCommand extends AbstractDataCommand implements DataWriteCommand, RemoteLockCommand {

   protected CommandInvocationId commandInvocationId;

   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key, int segment, long flagsBitSet, CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet);
      this.commandInvocationId = commandInvocationId;
   }

   @Override
   public Collection<?> getAffectedKeys() {
      return Collections.singleton(key);
   }

   @Override
   public boolean isReturnValueExpected() {
      return !hasAnyFlag(FlagBitSets.SKIP_REMOTE_LOOKUP | FlagBitSets.IGNORE_RETURN_VALUES);
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public Collection<?> getKeysToLock() {
      return getAffectedKeys();
   }

   @Override
   public final Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public final boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public final boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING);
   }

   @Override
   public CommandInvocationId getCommandInvocationId() {
      return commandInvocationId;
   }
}
