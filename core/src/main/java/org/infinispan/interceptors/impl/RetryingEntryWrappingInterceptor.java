package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationExceptionFunction;
import org.infinispan.interceptors.distribution.ConcurrentChangeException;
import org.infinispan.interceptors.distribution.ScatteredDistributionInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Used in @{link org.infinispan.configuration.cache.CacheMode#SCATTERED_SYNC scattered cache}
 * The commit is executed in {@link ScatteredDistributionInterceptor}
 * before replicating the change from primary owner.
 *
 * When the {@link ScatteredDistributionInterceptor} throws a {@link ConcurrentChangeException} during single-key
 * command processing, we know that the entry has not been committed and can safely remove the whole entry from context
 * and retry.
 * When the command processes multiple keys, some of the entries might be already committed. Therefore we have to keep
 * the original value in a {@link org.infinispan.container.entries.RepeatableReadEntry} and for committed entries we
 * only reset the value before retry (we assume that the outcome of an operation is deterministic). The non-committed
 * entries are removed and re-wrapped as in the single-key case.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class RetryingEntryWrappingInterceptor extends EntryWrappingInterceptor {
   private static final Log log = LogFactory.getLog(EntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationExceptionFunction handleDataWriteReturn = this::handleDataWriteReturn;
   private final InvocationExceptionFunction handleManyWriteReturn = this::handleManyWriteReturn;

   @Override
   protected Object setSkipRemoteGetsAndInvokeNextForDataCommand(InvocationContext ctx, DataWriteCommand command) {
      return invokeNextAndExceptionally(ctx, command, handleDataWriteReturn);
   }

   Object handleDataWriteReturn(InvocationContext ctx, VisitableCommand command, Throwable throwable) throws Throwable {
      if (throwable instanceof ConcurrentChangeException) {
         if (trace) {
            log.tracef(throwable, "Retrying %s after concurrent change", command);
         }
         DataWriteCommand dataWriteCommand = (DataWriteCommand) command;
         ctx.removeLookedUpEntry(dataWriteCommand.getKey());
         return visitCommand(ctx, dataWriteCommand);
      } else {
         throw throwable;
      }
   }

   @Override
   protected Object setSkipRemoteGetsAndInvokeNextForManyEntriesCommand(InvocationContext ctx, WriteCommand command) {
      return invokeNextAndExceptionally(ctx, command, handleManyWriteReturn);
   }

   Object handleManyWriteReturn(InvocationContext ctx, VisitableCommand command, Throwable throwable) throws Throwable {
      if (throwable instanceof ConcurrentChangeException) {
         if (trace) {
            log.tracef(throwable, "Retrying %s after concurrent change", command);
         }
         // Note: this is similar to what EWI does when RETRY flag is set, but we have to check entry.isCommitted()
         for (Object key : ((WriteCommand) command).getAffectedKeys()) {
            MVCCEntry entry = (MVCCEntry) ctx.lookupEntry(key);
            if (entry.isCommitted()) {
               entry.resetCurrentValue();
            } else {
               ctx.removeLookedUpEntry(key);
            }
         }
         return visitCommand(ctx, command);
      } else {
         throw throwable;
      }
   }
}
