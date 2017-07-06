package org.hibernate.test.cache.infinispan.util;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;


public class ExpectingInterceptor extends BaseCustomAsyncInterceptor {
   private final static Log log = LogFactory.getLog(ExpectingInterceptor.class);
   private final List<Condition> conditions = new LinkedList<>();

   public static ExpectingInterceptor get(AdvancedCache cache) {
      ExpectingInterceptor self = cache.getAsyncInterceptorChain().findInterceptorWithClass(ExpectingInterceptor.class);
      if (self != null) {
         return self;
      }
      ExpectingInterceptor ei = new ExpectingInterceptor();
      // We are adding this after ICI because we want to handle silent failures, too
      cache.getAsyncInterceptorChain().addInterceptorAfter(ei, InvocationContextInterceptor.class);
      return ei;
   }

   public static void cleanup(AdvancedCache... caches) {
      for (AdvancedCache c : caches) c.getAsyncInterceptorChain().removeInterceptor(ExpectingInterceptor.class);
   }

   public synchronized Condition when(BiPredicate<InvocationContext, VisitableCommand> predicate) {
      Condition condition = new Condition(predicate, source(), null);
      conditions.add(condition);
      return condition;
   }

   public synchronized Condition whenFails(BiPredicate<InvocationContext, VisitableCommand> predicate) {
      Condition condition = new Condition(predicate, source(), Boolean.FALSE);
      conditions.add(condition);
      return condition;
   }

   private static String source() {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      StackTraceElement ste = stackTrace[3];
      return ste.getFileName() + ":" + ste.getLineNumber();
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      return invokeNextAndFinally( ctx, command, new InvocationFinallyAction() {
         @Override
         public void accept(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable)
               throws Throwable {
            boolean succeeded = throwable == null;
            log.tracef("After command(successful=%s) %s", succeeded, rCommand);
            List<Runnable> toExecute = new ArrayList<>();
            synchronized (ExpectingInterceptor.this) {
               for (Iterator<Condition> iterator = conditions.iterator(); iterator.hasNext(); ) {
                  Condition condition = iterator.next();
                  log.tracef("Testing condition %s", condition);
                  if ((condition.success == null || condition.success == succeeded) && condition.predicate.test(rCtx, rCommand)) {
                     assert condition.action != null;
                     log.trace("Condition succeeded");
                     toExecute.add(condition.action);
                     if (condition.removeCheck == null || condition.removeCheck.getAsBoolean()) {
                        iterator.remove();
                     }
                  } else {
                     log.trace("Condition test failed");
                  }
               }
            }
            // execute without holding the lock
            for (Runnable runnable : toExecute) {
               log.tracef("Executing %s", runnable);
               runnable.run();
            }
         }
      } );
   }

   public class Condition {
      private final BiPredicate<InvocationContext, VisitableCommand> predicate;
      private final String source;
      private final Boolean success;
      private BooleanSupplier removeCheck;
      private Runnable action;

      public Condition(BiPredicate<InvocationContext, VisitableCommand> predicate, String source, Boolean success) {
         this.predicate = predicate;
         this.source = source;
         this.success = success;
      }

      public Condition run(Runnable action) {
         assert this.action == null;
         this.action = action;
         return this;
      }

      public Condition countDown(CountDownLatch latch) {
         return run(() -> latch.countDown()).removeWhen(() -> latch.getCount() == 0);
      }

      public Condition removeWhen(BooleanSupplier check) {
         assert this.removeCheck == null;
         this.removeCheck = check;
         return this;
      }

      public void cancel() {
         synchronized (ExpectingInterceptor.this) {
            conditions.remove(this);
         }
      }

      @Override
      public String toString() {
         final StringBuilder sb = new StringBuilder("Condition{");
         sb.append("source=").append(source);
         sb.append(", predicate=").append(predicate);
         sb.append(", success=").append(success);
         sb.append(", action=").append(action);
         sb.append('}');
         return sb.toString();
      }
   }
}
