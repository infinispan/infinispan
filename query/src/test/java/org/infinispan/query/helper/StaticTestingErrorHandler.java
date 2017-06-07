package org.infinispan.query.helper;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;

import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.WrappingErrorHandler;

public class StaticTestingErrorHandler implements ErrorHandler {

   private final AtomicReference faulty = new AtomicReference();

   @Override
   public void handle(ErrorContext context) {
      faulty.compareAndSet(null, new ThrowableWrapper(context.getOperationAtFault().toString(), context.getThrowable()));
   }

   @Override
   public void handleException(String errorMsg, Throwable exception) {
      faulty.compareAndSet(null, new ThrowableWrapper(errorMsg, exception));
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
      SearchManager searchManager = Search.getSearchManager(cache);
      SearchIntegrator searchFactory = searchManager.unwrap(SearchIntegrator.class);
      ErrorHandler errorHandler = searchFactory.getErrorHandler();
      assertNotNull(errorHandler);
      if (errorHandler instanceof WrappingErrorHandler) {
         errorHandler = ((WrappingErrorHandler) errorHandler).unwrap();
      }
      assertTrue(errorHandler instanceof StaticTestingErrorHandler);
      return (StaticTestingErrorHandler) errorHandler;
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
