package org.infinispan.query.backend;

import org.hibernate.search.spi.SearchIntegrator;

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

   private final SearchIntegrator searchFactory;
   private final QueryKnownClasses queryKnownClasses;
   private final TransactionHelper transactionHelper;

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
            handleOnDemandRegistration(potentialNewType);
            Boolean isIndexable = queryKnownClasses.get(potentialNewType);
            return isIndexable != null ? isIndexable : false;
         }
      } else {
         return false;
      }
   }

   private void handleOnDemandRegistration(Class<?>... classes) {
      List<Class<?>> reducedSet = new ArrayList<>(classes.length);
      for (Class<?> type : classes) {
         if (!queryKnownClasses.containsKey(type)) {
            reducedSet.add(type);
         }
      }
      if (!reducedSet.isEmpty()) {
         Class<?>[] toAdd = reducedSet.toArray(new Class[reducedSet.size()]);
         updateSearchFactory(toAdd);
         updateClusterRegistry(toAdd);
      }
   }

   private void updateClusterRegistry(final Class<?>... classes) {
      for (Class<?> c : classes) {
         queryKnownClasses.put(c, isIndexed(c));
      }
   }

   private void updateSearchFactory(final Class<?>... classes) {
      mutating.lock();
      try {
         //Need to re-filter the new types while holding the lock
         final List<Class<?>> reducedSet = new ArrayList<>(classes.length);
         for (Class<?> type : classes) {
            if (!isIndexed(type)) {
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
      } finally {
         mutating.unlock();
      }
   }

   boolean isIndexed(final Class<?> c) {
      return this.searchFactory.getIndexBinding(c) != null;
   }

   void handleClusterRegistryRegistration(final Class<?> clazz) {
      if (isIndexed(clazz)) {
         return;
      }
      updateSearchFactory(clazz);
   }

   void enableClasses(Class[] classes) {
      handleOnDemandRegistration(classes);
   }

}
