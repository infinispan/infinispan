package org.infinispan.query.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.Transaction;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wrapper around SearchIntegrator with guards to allow concurrent access
 *
 * @author gustavonalle
 * @since 7.0
 */
final class SearchFactoryHandler {

   private static final Log log = LogFactory.getLog(SearchFactoryHandler.class, Log.class);

   private final SearchIntegrator searchFactory;
   private final QueryKnownClasses queryKnownClasses;
   private final TransactionHelper transactionHelper;
   private final Executor listenerExecutor;
   private final ClassLoader classLoader;
   private final Object cacheListener = new CacheListener();

   private final ReentrantLock mutating = new ReentrantLock();

   SearchFactoryHandler(final SearchIntegrator searchFactory,
                        final QueryKnownClasses queryKnownClasses,
                        final TransactionHelper transactionHelper,
                        final Executor executor,
                        final ClassLoader classLoader) {
      this.searchFactory = searchFactory;
      this.queryKnownClasses = queryKnownClasses;
      this.transactionHelper = transactionHelper;
      this.listenerExecutor = executor;
      this.classLoader = classLoader;
   }

   boolean updateKnownTypesIfNeeded(final Object value) {
      if (value != null) {
         final Class<?> potentialNewType = value.getClass();
         final Boolean existingBoolean = queryKnownClasses.get(potentialNewType);
         if (existingBoolean != null) {
            return existingBoolean;
         } else {
            handleOnDemandRegistration(false, potentialNewType);
            Boolean isIndexable = queryKnownClasses.get(potentialNewType);
            return isIndexable != null ? isIndexable : false;
         }
      } else {
         return false;
      }
   }

   private void handleOnDemandRegistration(boolean allowUndeclared, Class<?>... classes) {
      List<Class<?>> reducedSet = new ArrayList<>(classes.length);
      for (Class<?> type : classes) {
         if (!queryKnownClasses.containsKey(type)) {
            reducedSet.add(type);
         }
      }
      if (!reducedSet.isEmpty()) {
         if (queryKnownClasses.isAutodetectEnabled()) {
            Class<?>[] toAdd = reducedSet.toArray(new Class[reducedSet.size()]);
            updateSearchFactoryInCallingThread(toAdd);
            for (Class<?> c : toAdd) {
               queryKnownClasses.put(c, hasIndex(c));
            }
         } else if (!allowUndeclared) {
            log.detectedUnknownIndexedEntities(queryKnownClasses.getCacheName(), reducedSet.toString());
         }
      }
   }

   private CompletionStage<Void> updateSearchFactory(final Class<?>... classes) {
      return CompletableFuture.runAsync(() -> updateSearchFactoryInCallingThread(classes), listenerExecutor);
   }

   private void updateSearchFactoryInCallingThread(final Class<?>... classes) {
      mutating.lock();
      try {
         //Need to re-filter the new types while holding the lock
         final List<Class<?>> reducedSet = new ArrayList<>(classes.length);
         for (Class<?> type : classes) {
            if (!hasIndex(type)) {
               reducedSet.add(type);
            }
         }
         if (reducedSet.isEmpty()) {
            return;
         }
         final Class<?>[] newtypes = reducedSet.toArray(new Class<?>[reducedSet.size()]);

         Transaction tx = transactionHelper.suspendTxIfExists();
         try {
            searchFactory.addClasses(newtypes);
         } finally {
            transactionHelper.resume(tx);
         }
         for (Class<?> type : newtypes) {
            if (hasIndex(type)) {
               log.detectedUnknownIndexedEntity(queryKnownClasses.getCacheName(), type.getName());
            }
         }
      } finally {
         mutating.unlock();
      }
   }

   /**
    * Checks if an index exists for the given class. This is not intended to test whether the entity class is indexable
    * (via annotations or programmatically).
    *
    * @param c the class to check
    * @return true if an index exists, false otherwise
    */
   boolean hasIndex(final Class<?> c) {
      return searchFactory.getIndexBindings().get(c) != null;
   }

   private CompletionStage<Void> handleClusterRegistryRegistration(QueryKnownClasses.KnownClassKey knownClassKey) {
      Class<?> clazz = knownClassKey.getKnownClass(classLoader);
      if (hasIndex(clazz)) {
         return null;
      }
      return updateSearchFactory(clazz);
   }

   void enableClasses(Class[] classes) {
      handleOnDemandRegistration(true, classes);
   }

   Object getCacheListener() {
      return cacheListener;
   }

   /**
    * A listener used to update the SearchFactoryHandler when an indexable class is added to cache.
    */
   @Listener
   final class CacheListener {

      @CacheEntryCreated
      public CompletionStage<Void> created(CacheEntryCreatedEvent<QueryKnownClasses.KnownClassKey, Boolean> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getValue()) {
            return handleClusterRegistryRegistration(e.getKey());
         }
         return null;
      }

      @CacheEntryModified
      public CompletionStage<Void> modified(CacheEntryModifiedEvent<QueryKnownClasses.KnownClassKey, Boolean> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getValue()) {
            return handleClusterRegistryRegistration(e.getKey());
         }
         return null;
      }
   }
}
