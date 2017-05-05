package org.infinispan.commands.write;

import java.util.Collection;
import java.util.Collections;

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
   protected transient InvocationManager invocationManager;
   protected transient Object providedResult;
   protected transient boolean completed;
   protected transient boolean authoritative;
   protected transient boolean synchronous;

   protected AbstractDataWriteCommand() {
   }

   protected AbstractDataWriteCommand(Object key, long flagsBitSet, CommandInvocationId commandInvocationId, Object providedResult, InvocationManager invocationManager, boolean synchronous) {
      super(key, flagsBitSet);
      this.commandInvocationId = commandInvocationId;
      this.providedResult = providedResult;
      this.invocationManager = invocationManager;
      this.synchronous = synchronous;
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

   protected void recordInvocation(InvocationContext ctx, CacheEntry e, Object result) {
      recordInvocation(ctx, e, result, EmbeddedMetadata.Builder.from(e.getMetadata()));
   }

   protected void recordInvocation(InvocationContext ctx, CacheEntry e, Object result, Metadata.Builder builder) {
      if ((synchronous && ctx.isOriginLocal()) || commandInvocationId == null || invocationManager == null) {
         e.setMetadata(builder.build());
         return;
      }
      if (e.isRemoved()) {
         builder = builder.maxIdle(-1).lifespan(invocationManager.invocationTimeout()).version(null);
      }
      if (!hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
         long now = invocationManager.wallClockTime();
         InvocationRecord purged = InvocationRecord.purgeExpired(builder.invocations(), now - invocationManager.invocationTimeout());
         builder = builder.invocations(purged).invocation(commandInvocationId, isReturnValueExpected() ? result : null, authoritative,
               e.isCreated(), !e.isCreated() && !e.isRemoved(), e.isRemoved(), now);
      }
      e.setMetadata(builder.build());
   }

   public void init(InvocationManager invocationManager, boolean synchronous) {
      this.invocationManager = invocationManager;
      this.synchronous = synchronous;
   }
}
