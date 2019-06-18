package org.infinispan.xsite.backupfailure;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.xsite.AbstractTwoSitesTest;
import org.testng.annotations.BeforeMethod;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class BaseBackupFailureTest extends AbstractTwoSitesTest {

   protected FailureInterceptor failureInterceptor;

   @Override
   protected void createSites() {
      super.createSites();
      failureInterceptor = new FailureInterceptor();
      backup(LON).getAdvancedCache().getAsyncInterceptorChain().addInterceptor(failureInterceptor, 1);
   }

   @BeforeMethod
   void resetFailureInterceptor() {
      failureInterceptor.reset();
   }

   public static class FailureInterceptor extends DDAsyncInterceptor {

      protected volatile boolean isFailing = false;

      protected volatile boolean rollbackFailed;
      protected volatile boolean commitFailed;
      protected volatile boolean prepareFailed;
      protected volatile boolean putFailed;
      protected volatile boolean removeFailed;
      protected volatile boolean clearFailed;
      protected volatile boolean writeOnlyManyEntriesFailed;

      public void reset() {
         rollbackFailed = false;
         commitFailed = false;
         prepareFailed = false;
         putFailed = false;
         removeFailed = false;
         clearFailed = false;
         writeOnlyManyEntriesFailed = false;
         isFailing = false;
      }

      private Object handle(InvocationContext ctx, VisitableCommand command, Runnable logFailure) throws Throwable {
         if (isFailing) {
            logFailure.run();
            throw new CacheException("Induced failure");
         } else {
            return invokeNext(ctx, command);
         }
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         return handle(ctx, command, () -> rollbackFailed = true);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         return handle(ctx, command, () -> commitFailed = true);
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         return handle(ctx, command, () -> prepareFailed = true);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         return handle(ctx, command, () -> putFailed = true);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         return handle(ctx, command, () -> removeFailed = true);
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         return handle(ctx, command, () -> clearFailed = true);
      }

      @Override
      public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
         return handle(ctx, command, () -> writeOnlyManyEntriesFailed = true);
      }

      public void disable() {
         isFailing = false;
      }

      public void enable() {
         isFailing = true;
      }
   }

   protected boolean failOnBackupFailure(String site, int cacheIndex) {
      return cache(site, cacheIndex).getCacheConfiguration().sites().allBackups().get(0).backupFailurePolicy() == BackupFailurePolicy.FAIL;
   }
}
