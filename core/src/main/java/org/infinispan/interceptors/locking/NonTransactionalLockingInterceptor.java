package org.infinispan.interceptors.locking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Locking interceptor to be used for non-transactional caches.
 *
 * @author Mircea Markus
 */
public class NonTransactionalLockingInterceptor extends AbstractLockingInterceptor {
   private static final Log log = LogFactory.getLog(NonTransactionalLockingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected final Object visitDataReadCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   protected Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      assertNonTransactional(ctx);
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) {
      // Non-transactional invalidation caches only lock the key on the originator
      if (!ctx.isOriginLocal())
         return invokeNext(ctx, command);

      return super.visitInvalidateCommand(ctx, command);
   }

   @Override
   protected Object handleReadManyCommand(InvocationContext ctx, FlagAffectedCommand command, Collection<?> keys) {
      assertNonTransactional(ctx);
      return invokeNext(ctx, command);
   }

   @Override
   protected <K> Object handleWriteManyCommand(InvocationContext ctx, WriteCommand command, Collection<K> keys, boolean forwarded) throws Throwable {
      assertNonTransactional(ctx);
      if (forwarded || hasSkipLocking(command)) {
         return invokeNext(ctx, command);
      }
      List<K> keysToLock = Collections.emptyList();
      for (K key : keys) {
         if (shouldLockKey(key)) {
            if (keysToLock == Collections.emptyList()) {
               keysToLock = new ArrayList<>();
            }
            keysToLock.add(key);
         }
      }
      InvocationStage lockStage = lockAllAndRecord(ctx, command, keysToLock, getLockTimeoutMillis(command));
      return nonTxLockAndInvokeNext(ctx, command, lockStage, unlockAllReturnHandler);
   }

   private void assertNonTransactional(InvocationContext ctx) {
      //this only happens if the cache is used in a transaction's scope
      if (ctx.isInTxScope()) {
         throw new InvalidCacheUsageException(
               "This is a non-transactional cache and cannot be accessed with a transactional InvocationContext.");
      }
   }
}
