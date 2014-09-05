package org.infinispan.query.backend;

import org.hibernate.search.backend.spi.Worker;
import org.hibernate.search.spi.SearchFactoryIntegrator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static org.infinispan.query.backend.TransactionHelper.Operation;

/**
 * Wrapper around SearchFactoryIntegrator with guards to allow concurrent access
 *
 * @author gustavonalle
 * @since 7.0
 */
final class SearchFactoryHandler {

   private final SearchFactoryIntegrator searchFactory;
   private final ReadIntensiveClusterRegistryWrapper<String, Class<?>, Boolean> clusterRegistry;
   private final TransactionHelper transactionHelper;

   private final ReentrantLock mutating = new ReentrantLock();

   SearchFactoryHandler(final SearchFactoryIntegrator searchFactory,
                        final ReadIntensiveClusterRegistryWrapper<String, Class<?>, Boolean> clusterRegistry,
                        final TransactionHelper transactionHelper) {
      this.searchFactory = searchFactory;
      this.clusterRegistry = clusterRegistry;
      this.transactionHelper = transactionHelper;
   }

   boolean updateKnownTypesIfNeeded(final Object value) {
      if (value != null) {
         final Class<?> potentialNewType = value.getClass();
         final Boolean existingBoolean = clusterRegistry.get(potentialNewType);
         if (existingBoolean != null) {
            return existingBoolean.booleanValue();
         }
         else {
            handleOnDemandRegistration(potentialNewType);
            return clusterRegistry.get(potentialNewType);
         }
      } else {
         return false;
      }
   }

   private void handleOnDemandRegistration(Class<?>... classes) {
      final Class<?>[] toAdd = filterAlreadyIndexed(classes);
      if (toAdd.length == 0) {
         return;
      }
      updateSearchFactory(toAdd);
      updateClusterRegistry(toAdd);
   }

   private void updateClusterRegistry(final Class<?>... classes) {
      for (Class<?> c : classes) {
         clusterRegistry.put(c, isIndexed(c));
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
         final Class<?>[] newtypes = new Class<?>[reducedSet.size()];
         reducedSet.toArray(newtypes);
         transactionHelper.runSuspendingTx(new Operation() {
            @Override
            public void execute() {
               // we need to preserve the state of this flag manually because addClasses will cause reconfiguration and the flag is lost
               boolean isStatisticsEnabled = searchFactory.getStatistics().isStatisticsEnabled();
               searchFactory.addClasses(newtypes);
               searchFactory.getStatistics().setStatisticsEnabled(isStatisticsEnabled);
            }
         });
      } finally {
         mutating.unlock();
      }
   }

   public SearchFactoryIntegrator getSearchFactory() {
      return searchFactory;
   }

   Worker getWorker() {
      return searchFactory.getWorker();
   }

   boolean isIndexed(final Class<?> c) {
      return this.searchFactory.getIndexBinding(c) != null;
   }

   private Class<?>[] filterAlreadyIndexed(Class... classes) {
      ArrayList<Class<?>> toAdd = new ArrayList<>(classes.length);
      for (Class<?> type : classes) {
         if (!clusterRegistry.containsKey(type)) {
            toAdd.add(type);
         }
      }
      return toAdd.toArray(new Class[toAdd.size()]);
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
