package org.infinispan.commands.write;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.locks.RemoteLockCommand;

/**
 * Stuff common to WriteCommands
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractDataWriteCommand extends AbstractDataCommand implements DataWriteCommand, RemoteLockCommand {

   protected CommandInvocationId commandInvocationId;
   protected transient Object providedResult;
   protected transient boolean completed;
   protected transient boolean authoritative;

   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key, long flagsBitSet, CommandInvocationId commandInvocationId, Object providedResult) {
      super(key, flagsBitSet);
      this.commandInvocationId = commandInvocationId;
      this.providedResult = providedResult;
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

   @Override
   public void setAuthoritative(boolean authoritative) {
      this.authoritative = authoritative;
   }

   @Override
   public void setCompleted(Object key) {
      // TODO: remove this and use isExecuted
      addFlags(FlagBitSets.SKIP_CACHE_STORE);
      this.completed = true;
   }

   @Override
   public boolean isCompleted(Object key) {
      return completed;
   }

   protected void recordInvocation(CacheEntry e, Object result) {
      recordInvocation(e, result, EmbeddedMetadata.Builder.from(e.getMetadata()));
   }

   protected void recordInvocation(CacheEntry e, Object result, Metadata.Builder builder) {
      if (commandInvocationId == null) {
         e.setMetadata(builder.build());
         return;
      }
      // TODO: set lifespan according to invocation GC settings
      if (e.isRemoved()) {
         builder = builder.maxIdle(-1).lifespan(-1).version(null);
      }
      if (!hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         builder = builder.invocation(commandInvocationId, result, authoritative,
               e.isCreated(), !e.isCreated() && !e.isRemoved(), e.isRemoved(), 0);
      }
      e.setMetadata(builder.build());
   }
}
