package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.context.Flag;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

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

   protected AbstractDataWriteCommand(Object key, Set<Flag> flags, CommandInvocationId commandInvocationId) {
      super(key, flags);
      this.commandInvocationId = commandInvocationId;
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return Collections.singleton(key);
   }

   @Override
   public boolean isReturnValueExpected() {
      return !hasFlag(Flag.SKIP_REMOTE_LOOKUP) && !hasFlag(Flag.IGNORE_RETURN_VALUES);
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   @Override
   public Collection<Object> getKeysToLock() {
      return getAffectedKeys();
   }

   @Override
   public final Object getKeyLockOwner() {
      return commandInvocationId;
   }

   @Override
   public final boolean hasZeroLockAcquisition() {
      return hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public final boolean hasSkipLocking() {
      return hasFlag(Flag.SKIP_LOCKING);
   }
}
