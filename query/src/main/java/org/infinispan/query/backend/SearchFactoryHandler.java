package org.infinispan.query.backend;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.query.logging.Log;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.query.backend.TransactionHelper.Operation;

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
   private final Object cacheListener = new CacheListener();

   private final ReentrantLock mutating = new ReentrantLock();

   SearchFactoryHandler(final SearchIntegrator searchFactory,
                        final QueryKnownClasses queryKnownClasses,
                        final TransactionHelper transactionHelper) {
      this.searchFactory = searchFactory;
      this.queryKnownClasses = queryKnownClasses;
      this.transactionHelper = transactionHelper;
   }

   boolean updateKnownTypesIfNeeded(final Object value) {
      if (value != null) {
         final Class<?> potentialNewType = value.getClass();
         final Boolean existingBoolean = queryKnownClasses.get(potentialNewType);
         if (existingBoolean != null) {
            return existingBoolean;
         }
         else {
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
            updateSearchFactory(toAdd);
            for (Class<?> c : toAdd) {
               queryKnownClasses.put(c, hasIndex(c));
            }
         } else if (!allowUndeclared) {
            log.detectedUnknownIndexedEntities(queryKnownClasses.getCacheName(), reducedSet.toString());
         }
      }
   }

   private void updateSearchFactory(final Class<?>... classes) {
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
         transactionHelper.runSuspendingTx(new Operation() {
            @Override
            public void execute() {
               searchFactory.addClasses(newtypes);
            }
         });
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
      return searchFactory.getIndexBinding(c) != null;
   }

   private void handleClusterRegistryRegistration(final Class<?> clazz) {
      if (hasIndex(clazz)) {
         return;
      }
      updateSearchFactory(clazz);
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
      public void created(CacheEntryCreatedEvent<KeyValuePair<String, Class>, Boolean> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getValue()) {
            handleClusterRegistryRegistration(e.getKey().getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<KeyValuePair<String, Class>, Boolean> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getValue()) {
            handleClusterRegistryRegistration(e.getKey().getValue());
         }
      }
   }
}
