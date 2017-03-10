package org.infinispan.commands.write;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commands.read.AbstractDataCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
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
   protected CommandInvocationId lastInvocationId;
   protected transient InvocationManager invocationManager;
   protected transient boolean completed;

   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key, long flagsBitSet, CommandInvocationId commandInvocationId, InvocationManager invocationManager) {
      super(key, flagsBitSet);
      this.commandInvocationId = commandInvocationId;
      this.invocationManager = invocationManager;
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
   public void setCompleted(Object key, boolean isCompleted) {
      assert Objects.equals(key, this.key);
      this.completed = isCompleted;
   }

   @Override
   public boolean isCompleted(Object key) {
      return completed;
   }

   @Override
   public Stream<?> completedKeys() {
      return completed ? Stream.of(key) : Stream.empty();
   }

   protected void recordInvocation(InvocationContext ctx, CacheEntry e, Object previousValue, Metadata previousMetadata) {
      recordInvocation(ctx, e, previousValue, previousMetadata, EmbeddedMetadata.Builder.from(e.getMetadata()));
   }

   protected void recordInvocation(InvocationContext ctx, CacheEntry e, Object previousValue, Metadata previousMetadata, Metadata.Builder builder) {
      if (invocationManager == null || (invocationManager.isSynchronous() && ctx.isOriginLocal()) || commandInvocationId == null) {
         e.setMetadata(builder.build());
         return;
      }
      if (e.isRemoved()) {
         builder = builder.maxIdle(-1).lifespan(invocationManager.invocationTimeout()).version(null);
      }
      if (!hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         long now = invocationManager.wallClockTime();
         InvocationRecord purged = InvocationRecord.purgeExpired(builder.invocations(), now - invocationManager.invocationTimeout());
         builder = builder.invocations(purged).invocation(commandInvocationId, previousValue, previousMetadata, now);
      }
      e.setMetadata(builder.build());
   }

   @Override
   public CommandInvocationId getLastInvocationId(Object key) {
      assert Objects.equals(key, this.key);
      return lastInvocationId;
   }

   @Override
   public void setLastInvocationId(Object key, CommandInvocationId id) {
      assert Objects.equals(key, this.key);
      lastInvocationId = id;
   }
}
