package org.infinispan.query.helper;

import java.util.concurrent.atomic.AtomicBoolean;

import org.hibernate.search.exception.ErrorContext;
import org.hibernate.search.exception.ErrorHandler;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.junit.Assert;

public class StaticTestingErrorHandler implements ErrorHandler {

   private final AtomicBoolean faulty = new AtomicBoolean(false);
   private volatile String description = null;

   @Override
   public void handle(ErrorContext context) {
      faulty.set(true);
      this.description = context.getThrowable().toString();
   }

   @Override
   public void handleException(String errorMsg, Throwable exception) {
      faulty.set(true);
   }

   private boolean getAndReset() {
      return faulty.getAndSet(false);
   }

   public static void assertAllGood(Cache cache) {
      SearchManager searchManager = Search.getSearchManager(cache);
      SearchFactoryIntegrator searchFactory = (SearchFactoryIntegrator) searchManager.getSearchFactory();
      ErrorHandler errorHandler = searchFactory.getErrorHandler();
      Assert.assertNotNull(errorHandler);
      Assert.assertTrue(errorHandler instanceof StaticTestingErrorHandler);
      StaticTestingErrorHandler instance = (StaticTestingErrorHandler) errorHandler;
      Assert.assertFalse("Something failed in the indexing backend: " + instance.description, instance.getAndReset());
   }

   public static void assertAllGood(Cache... caches) {
      for (Cache cache : caches) {
         assertAllGood(cache);
      }
   }

}
