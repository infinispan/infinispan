package org.infinispan.query.helper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicReference;

import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.junit.Assert;

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
      SearchManager searchManager = Search.getSearchManager(cache);
      SearchFactoryIntegrator searchFactory = searchManager.getSearchFactory();
      ErrorHandler errorHandler = searchFactory.getErrorHandler();
      Assert.assertNotNull(errorHandler);
      Assert.assertTrue(errorHandler instanceof StaticTestingErrorHandler);
      StaticTestingErrorHandler instance = (StaticTestingErrorHandler) errorHandler;
      Object fault = instance.getAndReset();
      if (fault!=null) {
         Assert.fail(fault.toString());
      }
   }

   public static void assertAllGood(Cache... caches) {
      for (Cache cache : caches) {
         assertAllGood(cache);
      }
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
