package org.infinispan.factories;


import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.CustomInterceptorsConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.TriangleAckInterceptor;
import org.infinispan.interceptors.distribution.DistributionBulkInterceptor;
import org.infinispan.interceptors.distribution.L1LastChanceInterceptor;
import org.infinispan.interceptors.distribution.L1NonTxInterceptor;
import org.infinispan.interceptors.distribution.L1TxInterceptor;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.interceptors.impl.ActivationInterceptor;
import org.infinispan.interceptors.impl.AsyncInterceptorChainImpl;
import org.infinispan.interceptors.impl.BatchingInterceptor;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.impl.ClusteredActivationInterceptor;
import org.infinispan.interceptors.impl.ClusteredCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CompatibilityInterceptor;
import org.infinispan.interceptors.impl.DeadlockDetectingInterceptor;
import org.infinispan.interceptors.impl.DistCacheWriterInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.interceptors.impl.GroupingInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.interceptors.impl.IsMarshallableInterceptor;
import org.infinispan.interceptors.impl.NotificationInterceptor;
import org.infinispan.interceptors.impl.TransactionalStoreInterceptor;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderStateTransferInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.xsite.NonTransactionalBackupInterceptor;
import org.infinispan.interceptors.xsite.OptimisticBackupInterceptor;
import org.infinispan.interceptors.xsite.PessimisticBackupInterceptor;
import org.infinispan.partitionhandling.impl.PartitionHandlingInterceptor;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.statetransfer.TransactionSynchronizerInterceptor;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Factory class that builds an interceptor chain based on cache configuration.
 *
 * For backwards compatibility, the factory will register both a {@link AsyncInterceptorChain} and
 * a {@link InterceptorChain} before initializing the interceptors.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @author Marko Luksa
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = {AsyncInterceptorChain.class, InterceptorChain.class})
public class InterceptorChainFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(InterceptorChainFactory.class);

   private AsyncInterceptor createInterceptor(AsyncInterceptor interceptor,
         Class<? extends AsyncInterceptor> interceptorType) {
      AsyncInterceptor chainedInterceptor = componentRegistry.getComponent(interceptorType);
      if (chainedInterceptor == null) {
         register(interceptorType, interceptor);
         chainedInterceptor = interceptor;
      }
      return chainedInterceptor;
   }


   private void register(Class<? extends AsyncInterceptor> clazz, AsyncInterceptor chainedInterceptor) {
      try {
         componentRegistry.registerComponent(chainedInterceptor, clazz);
      } catch (RuntimeException e) {
         log.unableToCreateInterceptor(clazz, e);
         throw e;
      }
   }

   public AsyncInterceptorChain buildInterceptorChain() {
      TransactionMode transactionMode = configuration.transaction().transactionMode();
      boolean needsVersionAwareComponents = transactionMode.isTransactional() &&
              Configurations.isVersioningEnabled(configuration);

      AsyncInterceptorChain interceptorChain =
            new AsyncInterceptorChainImpl(componentRegistry.getComponentMetadataRepo());
      // add the interceptor chain to the registry first, since some interceptors may ask for it.
      // Add both the old class and the new interface
      componentRegistry.registerComponent(interceptorChain, AsyncInterceptorChain.class);
      componentRegistry.registerComponent(new InterceptorChain(interceptorChain), InterceptorChain.class);

      boolean invocationBatching = configuration.invocationBatching().enabled();
      boolean isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();
      CacheMode cacheMode = configuration.clustering().cacheMode();

      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         interceptorChain.appendInterceptor(createInterceptor(new DistributionBulkInterceptor<>(),
                 DistributionBulkInterceptor.class), false);
      }

      // load the icInterceptor first
      if (invocationBatching) {
         interceptorChain.appendInterceptor(createInterceptor(new BatchingInterceptor(), BatchingInterceptor.class), false);
      }
      interceptorChain.appendInterceptor(createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class), false);

      CompatibilityModeConfiguration compatibility = configuration.compatibility();
      if (compatibility.enabled()) {
         interceptorChain.appendInterceptor(createInterceptor(
               new CompatibilityInterceptor(), CompatibilityInterceptor.class), false);
      }

      // add marshallable check interceptor for situations where we want to figure out before marshalling
      // Store as binary marshalls keys/values eagerly now, so avoid extra serialization
      if (hasAsyncStore())
         interceptorChain.appendInterceptor(createInterceptor(new IsMarshallableInterceptor(), IsMarshallableInterceptor.class), false);

      // load the cache management interceptor next
      if (configuration.jmxStatistics().available()) {
         interceptorChain.appendInterceptor(createInterceptor(new CacheMgmtInterceptor(), CacheMgmtInterceptor.class), false);
      }

      // load the state transfer lock interceptor
      // the state transfer lock ensures that the cache member list is up-to-date
      // so it's necessary even if state transfer is disabled
      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         if (isTotalOrder) {
            interceptorChain.appendInterceptor(createInterceptor(new TotalOrderStateTransferInterceptor(),
                                                                 TotalOrderStateTransferInterceptor.class), false);
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new StateTransferInterceptor(), StateTransferInterceptor.class), false);
         }
         if (transactionMode.isTransactional()) {
            interceptorChain.appendInterceptor(createInterceptor(new TransactionSynchronizerInterceptor(), TransactionSynchronizerInterceptor.class), false);
         }
      }

      if (transactionMode == TransactionMode.NON_TRANSACTIONAL && cacheMode.isDistributed()) {
         interceptorChain.appendInterceptor(createInterceptor(new TriangleAckInterceptor(), TriangleAckInterceptor.class), false);
      }

      // The partition handling must run every time the state transfer interceptor retries a command (in non-transactional caches)
      if (configuration.clustering().partitionHandling().enabled()
            && (cacheMode.isDistributed() || cacheMode.isReplicated())) {
         interceptorChain.appendInterceptor(createInterceptor(new PartitionHandlingInterceptor(), PartitionHandlingInterceptor.class), false);
      }


      //load total order interceptor
      if (isTotalOrder) {
         interceptorChain.appendInterceptor(createInterceptor(new TotalOrderInterceptor(), TotalOrderInterceptor.class), false);
      }

      // load the tx interceptor
      if (transactionMode.isTransactional())
         interceptorChain.appendInterceptor(createInterceptor(new TxInterceptor(), TxInterceptor.class), false);


      if (configuration.transaction().useEagerLocking()) {
         configuration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      //the total order protocol doesn't need locks
      if (!isTotalOrder) {
         if (transactionMode.isTransactional()) {
            if (configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC) {
               interceptorChain.appendInterceptor(createInterceptor(new PessimisticLockingInterceptor(), PessimisticLockingInterceptor.class), false);
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new OptimisticLockingInterceptor(), OptimisticLockingInterceptor.class), false);
            }
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new NonTransactionalLockingInterceptor(), NonTransactionalLockingInterceptor.class), false);
         }
      }

      // NotificationInterceptor is used only for Prepare/Commit/Rollback notifications
      // This needs to be after locking interceptor to guarantee that locks are still held when raising notifications
      if (transactionMode.isTransactional() && configuration.transaction().notifications()) {
         interceptorChain.appendInterceptor(createInterceptor(new NotificationInterceptor(), NotificationInterceptor.class), false);
      }

      if (configuration.sites().hasEnabledBackups() && !configuration.sites().disableBackups()) {
         if (transactionMode == TransactionMode.TRANSACTIONAL) {
            if (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
               interceptorChain.appendInterceptor(createInterceptor(new OptimisticBackupInterceptor(), OptimisticBackupInterceptor.class), false);
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new PessimisticBackupInterceptor(), PessimisticBackupInterceptor.class), false);
            }
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new NonTransactionalBackupInterceptor(), NonTransactionalBackupInterceptor.class), false);
         }
      }

      // This needs to be added after the locking interceptor (for tx caches) but before the wrapping interceptor.
      if (configuration.clustering().l1().enabled()) {
         interceptorChain.appendInterceptor(createInterceptor(new L1LastChanceInterceptor(), L1LastChanceInterceptor.class), false);
      }

      if (configuration.clustering().hash().groups().enabled()) {
         interceptorChain.appendInterceptor(createInterceptor(new GroupingInterceptor(), GroupingInterceptor.class), false);
      }

      if (needsVersionAwareComponents && cacheMode.isClustered()) {
         if (isTotalOrder) {
            interceptorChain.appendInterceptor(createInterceptor(new TotalOrderVersionedEntryWrappingInterceptor(),
                                                                 TotalOrderVersionedEntryWrappingInterceptor.class), false);
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new VersionedEntryWrappingInterceptor(), VersionedEntryWrappingInterceptor.class), false);
         }
      } else
         interceptorChain.appendInterceptor(createInterceptor(new EntryWrappingInterceptor(), EntryWrappingInterceptor.class), false);

      if (configuration.persistence().usingStores()) {
         if (configuration.persistence().passivation()) {
            if (cacheMode.isClustered())
               interceptorChain.appendInterceptor(createInterceptor(new ClusteredActivationInterceptor(), ClusteredActivationInterceptor.class), false);
            else
               interceptorChain.appendInterceptor(createInterceptor(new ActivationInterceptor(), ActivationInterceptor.class), false);
         } else {
            if (cacheMode.isClustered())
               interceptorChain.appendInterceptor(createInterceptor(new ClusteredCacheLoaderInterceptor(), ClusteredCacheLoaderInterceptor.class), false);
            else
               interceptorChain.appendInterceptor(createInterceptor(new CacheLoaderInterceptor(), CacheLoaderInterceptor.class), false);

            boolean transactionalStore = configuration.persistence().stores().stream().anyMatch(StoreConfiguration::transactional);
            if (transactionalStore && transactionMode.isTransactional())
               interceptorChain.appendInterceptor(createInterceptor(new TransactionalStoreInterceptor(), TransactionalStoreInterceptor.class), false);

            switch (cacheMode) {
               case DIST_SYNC:
               case DIST_ASYNC:
               case REPL_SYNC:
               case REPL_ASYNC:
                  interceptorChain.appendInterceptor(createInterceptor(new DistCacheWriterInterceptor(), DistCacheWriterInterceptor.class), false);
                  break;
               default:
                  interceptorChain.appendInterceptor(createInterceptor(new CacheWriterInterceptor(), CacheWriterInterceptor.class), false);
                  break;
            }
         }
      }

      if (configuration.deadlockDetection().enabled() && !isTotalOrder) {
         interceptorChain.appendInterceptor(createInterceptor(new DeadlockDetectingInterceptor(), DeadlockDetectingInterceptor.class), false);
      }

      if (configuration.clustering().l1().enabled()) {
         if (transactionMode.isTransactional()) {
            interceptorChain.appendInterceptor(createInterceptor(new L1TxInterceptor(), L1TxInterceptor.class), false);
         }
         else {
            interceptorChain.appendInterceptor(createInterceptor(new L1NonTxInterceptor(), L1NonTxInterceptor.class), false);
         }
      }

      switch (cacheMode) {
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            interceptorChain.appendInterceptor(createInterceptor(new InvalidationInterceptor(), InvalidationInterceptor.class), false);
            break;
         case DIST_SYNC:
         case REPL_SYNC:
            if (needsVersionAwareComponents) {
               if (isTotalOrder) {
                  interceptorChain.appendInterceptor(createInterceptor(new TotalOrderVersionedDistributionInterceptor(),
                                                                       TotalOrderVersionedDistributionInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new VersionedDistributionInterceptor(), VersionedDistributionInterceptor.class), false);
               }
               break;
            }
         case DIST_ASYNC:
         case REPL_ASYNC:
            if (transactionMode.isTransactional()) {
               if (isTotalOrder) {
                  interceptorChain.appendInterceptor(createInterceptor(new TotalOrderDistributionInterceptor(), TotalOrderDistributionInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new TxDistributionInterceptor(), TxDistributionInterceptor.class), false);
               }
            } else {
               if (cacheMode.isDistributed()) {
                  interceptorChain.appendInterceptor(createInterceptor(new TriangleDistributionInterceptor(), TriangleDistributionInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new NonTxDistributionInterceptor(), NonTxDistributionInterceptor.class), false);
               }
            }
            break;
         case LOCAL:
            //Nothing...
      }

      AsyncInterceptor callInterceptor = createInterceptor(new CallInterceptor(), CallInterceptor.class);
      interceptorChain.appendInterceptor(callInterceptor, false);
      log.trace("Finished building default interceptor chain.");
      buildCustomInterceptors(interceptorChain, configuration.customInterceptors());
      return interceptorChain;
   }

   private void buildCustomInterceptors(AsyncInterceptorChain interceptorChain, CustomInterceptorsConfiguration customInterceptors) {
      for (InterceptorConfiguration config : customInterceptors.interceptors()) {
         if (interceptorChain.containsInterceptorType(config.asyncInterceptor().getClass())) continue;

         AsyncInterceptor customInterceptor = config.asyncInterceptor();
         SecurityActions.applyProperties(customInterceptor, config.properties());
         register(customInterceptor.getClass(), customInterceptor);
         if (config.first())
            interceptorChain.addInterceptor(customInterceptor, 0);
         else if (config.last())
            interceptorChain.addInterceptorBefore(customInterceptor, CallInterceptor.class);
         else if (config.index() >= 0)
            interceptorChain.addInterceptor(customInterceptor, config.index());
         else if (config.after() != null) {
            boolean added = interceptorChain.addInterceptorAfter(customInterceptor, config.after());
            if (!added) {
               throw new CacheConfigurationException("Cannot add after class: " + config.after()
                                                      + " as no such interceptor exists in the default chain");
            }
         } else if (config.before() != null) {
            boolean added = interceptorChain.addInterceptorBefore(customInterceptor, config.before());
            if (!added) {
               throw new CacheConfigurationException("Cannot add before class: " + config.before()
                                                      + " as no such interceptor exists in the default chain");
            }
         } else if (config.position() == InterceptorConfiguration.Position.OTHER_THAN_FIRST_OR_LAST) {
            interceptorChain.addInterceptor(customInterceptor, 1);
         }
      }

   }

   private boolean hasAsyncStore() {
      List<StoreConfiguration> loaderConfigs = configuration.persistence().stores();
      for (StoreConfiguration loaderConfig : loaderConfigs) {
         if (loaderConfig.async().enabled())
            return true;
      }
      return false;
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      try {
         AsyncInterceptorChain asyncInterceptorChain = buildInterceptorChain();
         if (componentType == InterceptorChain.class) {
            return componentType.cast(componentRegistry.getComponent(InterceptorChain.class));
         } else {
            return componentType.cast(asyncInterceptorChain);
         }
      } catch (CacheException ce) {
         throw ce;
      } catch (Exception e) {
         throw new CacheConfigurationException("Unable to build interceptor chain", e);
      }
   }

   public static InterceptorChainFactory getInstance(ComponentRegistry componentRegistry, Configuration configuration) {
      InterceptorChainFactory icf = new InterceptorChainFactory();
      icf.componentRegistry = componentRegistry;
      icf.configuration = configuration;
      return icf;
   }
}
