package org.infinispan.query.helper;

import static org.infinispan.query.helper.IndexAccessor.extractFailureHandler;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;
import org.hibernate.search.engine.reporting.EntityIndexingFailureContext;
import org.hibernate.search.engine.reporting.FailureContext;
import org.hibernate.search.engine.reporting.FailureHandler;
import org.infinispan.Cache;

public class StaticTestingErrorHandler implements FailureHandler {

   private final AtomicReference faulty = new AtomicReference();

   @Override
   public void handle(FailureContext context) {
      faulty.compareAndSet(null, new ThrowableWrapper(context.failingOperation().toString(), context.throwable()));
   }

   @Override
   public void handle(EntityIndexingFailureContext context) {
      faulty.compareAndSet(null, new ThrowableWrapper(context.failingOperation().toString(), context.throwable()));
   }

   private Object getAndReset() {
      return faulty.getAndSet(null);
   }

   public static void assertAllGood(Cache cache) {
      StaticTestingErrorHandler instance = extract(cache);
      instance.assertNoErrors();
   }

   public static void assertAllGood(Cache... caches) {
      for (Cache cache : caches) {
         assertAllGood(cache);
      }
   }

   private void assertNoErrors() {
      Object fault = getAndReset();
      if (fault != null) {
         fail(fault.toString());
      }
   }

   public static StaticTestingErrorHandler extract(Cache cache) {
      FailureHandler failureHandler = extractFailureHandler(cache);
      assertTrue(failureHandler instanceof StaticTestingErrorHandler);
      return (StaticTestingErrorHandler) failureHandler;
   }

   public static class ThrowableWrapper {

      private final String errorMsg;
      private final Throwable exception;

      public ThrowableWrapper(String errorMsg, Throwable exception) {
         this.errorMsg = errorMsg;
         this.exception = exception;
      }

      @Override
      public String toString() {
         StringWriter w = new StringWriter();
         w.append(String.valueOf(errorMsg));
         if (exception != null) {
            w.append(' ');
            exception.printStackTrace(new PrintWriter(w));
         }
         return w.toString();
      }
   }
}
