package org.infinispan.interceptors.locking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
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
      KeyAwareLockPromise lockPromise = lockAllAndRecord(ctx, keysToLock, getLockTimeoutMillis(command));
      return nonTxLockAndInvokeNext(ctx, command, lockPromise, unlockAllReturnHandler);
   }

   private void assertNonTransactional(InvocationContext ctx) {
      //this only happens if the cache is used in a transaction's scope
      if (ctx.isInTxScope()) {
         throw new InvalidCacheUsageException(
               "This is a non-transactional cache and cannot be accessed with a transactional InvocationContext.");
      }
   }
}
