package org.infinispan.factories;

import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.CustomInterceptorsConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.EmptyAsyncInterceptorChain;
import org.infinispan.interceptors.distribution.L1LastChanceInterceptor;
import org.infinispan.interceptors.distribution.L1NonTxInterceptor;
import org.infinispan.interceptors.distribution.L1TxInterceptor;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.interceptors.impl.AsyncInterceptorChainImpl;
import org.infinispan.interceptors.impl.BatchingInterceptor;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.impl.ClusteredCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.DistCacheWriterInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.interceptors.impl.IsMarshallableInterceptor;
import org.infinispan.interceptors.impl.NonTxIracLocalSiteInterceptor;
import org.infinispan.interceptors.impl.NonTxIracRemoteSiteInterceptor;
import org.infinispan.interceptors.impl.NotificationInterceptor;
import org.infinispan.interceptors.impl.OptimisticTxIracLocalSiteInterceptor;
import org.infinispan.interceptors.impl.PassivationCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.PassivationClusteredCacheLoaderInterceptor;
import org.infinispan.interceptors.impl.PassivationWriterInterceptor;
import org.infinispan.interceptors.impl.PessimisticTxIracLocalInterceptor;
import org.infinispan.interceptors.impl.TransactionalExceptionEvictionInterceptor;
import org.infinispan.interceptors.impl.TransactionalStoreInterceptor;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.interceptors.impl.VersionInterceptor;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.xsite.NonTransactionalBackupInterceptor;
import org.infinispan.interceptors.xsite.OptimisticBackupInterceptor;
import org.infinispan.interceptors.xsite.PessimisticBackupInterceptor;
import org.infinispan.partitionhandling.PartitionHandling;
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
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @author Marko Luksa
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = {AsyncInterceptorChain.class})
public class InterceptorChainFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(InterceptorChainFactory.class);

   private AsyncInterceptor createInterceptor(AsyncInterceptor interceptor,
         Class<? extends AsyncInterceptor> interceptorType) {
      ComponentRef<? extends AsyncInterceptor> chainedInterceptor = basicComponentRegistry.getComponent(interceptorType);
      if (chainedInterceptor != null) {
         return chainedInterceptor.wired();
      }

      // TODO Dan: could use wireDependencies, as dependencies on interceptors won't trigger a call to the chain factory anyway
      register(interceptorType, interceptor);
      return interceptor;
   }


   private void register(Class<? extends AsyncInterceptor> clazz, AsyncInterceptor chainedInterceptor) {
      try {
         basicComponentRegistry.registerComponent(clazz.getName(), chainedInterceptor, true);
         basicComponentRegistry.addDynamicDependency(AsyncInterceptorChain.class.getName(), clazz.getName());
      } catch (RuntimeException e) {
         log.unableToCreateInterceptor(clazz, e);
         throw e;
      }
   }

   private AsyncInterceptorChain buildInterceptorChain() {
      TransactionMode transactionMode = configuration.transaction().transactionMode();
      boolean needsVersionAwareComponents = Configurations.isTxVersioned(configuration);

      AsyncInterceptorChain interceptorChain = new AsyncInterceptorChainImpl();

      boolean invocationBatching = configuration.invocationBatching().enabled();
      CacheMode cacheMode = configuration.clustering().cacheMode();

      // load the icInterceptor first
      if (invocationBatching) {
         interceptorChain.appendInterceptor(createInterceptor(new BatchingInterceptor(), BatchingInterceptor.class), false);
      }
      interceptorChain.appendInterceptor(createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class), false);

      if (!configuration.transaction().transactionMode().isTransactional()) {
         interceptorChain.appendInterceptor(createInterceptor(new VersionInterceptor(), VersionInterceptor.class), false);
      }

      // add marshallable check interceptor for situations where we want to figure out before marshalling
      if (hasAsyncStore())
         interceptorChain.appendInterceptor(createInterceptor(new IsMarshallableInterceptor(), IsMarshallableInterceptor.class), false);

      // load the cache management interceptor next
      if (configuration.statistics().available()) {
         interceptorChain.appendInterceptor(createInterceptor(new CacheMgmtInterceptor(), CacheMgmtInterceptor.class), false);
      }

      // the state transfer interceptor sets the topology id and retries on topology changes
      // so it's necessary even if there is no state transfer
      // the only exception is non-tx invalidation mode, which ignores lock owners
      if (cacheMode.needsStateTransfer() || cacheMode.isInvalidation() && transactionMode.isTransactional()) {
         interceptorChain.appendInterceptor(createInterceptor(new StateTransferInterceptor(), StateTransferInterceptor.class), false);
      }

      if (cacheMode.needsStateTransfer()) {
         if (transactionMode.isTransactional()) {
            interceptorChain.appendInterceptor(createInterceptor(new TransactionSynchronizerInterceptor(), TransactionSynchronizerInterceptor.class), false);
         }
         if (configuration.clustering().partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES) {
            interceptorChain.appendInterceptor(createInterceptor(new PartitionHandlingInterceptor(), PartitionHandlingInterceptor.class), false);
         }
      }

      // load the tx interceptor
      if (transactionMode.isTransactional())
         interceptorChain.appendInterceptor(createInterceptor(new TxInterceptor<>(), TxInterceptor.class), false);

      if (transactionMode.isTransactional()) {
         if (configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC) {
            interceptorChain.appendInterceptor(createInterceptor(new PessimisticLockingInterceptor(), PessimisticLockingInterceptor.class), false);
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new OptimisticLockingInterceptor(), OptimisticLockingInterceptor.class), false);
         }
      } else {
         interceptorChain.appendInterceptor(createInterceptor(new NonTransactionalLockingInterceptor(), NonTransactionalLockingInterceptor.class), false);
      }

      // NotificationInterceptor is used only for Prepare/Commit/Rollback notifications
      // This needs to be after locking interceptor to guarantee that locks are still held when raising notifications
      if (transactionMode.isTransactional() && configuration.transaction().notifications()) {
         interceptorChain.appendInterceptor(createInterceptor(new NotificationInterceptor(), NotificationInterceptor.class), false);
      }

      if (configuration.sites().hasBackups()) {
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

      if (needsVersionAwareComponents) {
         interceptorChain.appendInterceptor(createInterceptor(new VersionedEntryWrappingInterceptor(), VersionedEntryWrappingInterceptor.class), false);
      } else {
         interceptorChain.appendInterceptor(createInterceptor(new EntryWrappingInterceptor(), EntryWrappingInterceptor.class), false);
      }

      // Has to be after entry wrapping interceptor so it can properly see context values even when removed
      if (transactionMode.isTransactional()) {
         if (configuration.memory().evictionStrategy().isExceptionBased()) {
            interceptorChain.appendInterceptor(createInterceptor(new TransactionalExceptionEvictionInterceptor(),
                  TransactionalExceptionEvictionInterceptor.class), false);
         }
      }

      if (configuration.persistence().usingStores()) {
         addPersistenceInterceptors(interceptorChain, configuration, configuration.persistence().stores());
      }

      if (configuration.clustering().l1().enabled()) {
         if (transactionMode.isTransactional()) {
            interceptorChain.appendInterceptor(createInterceptor(new L1TxInterceptor(), L1TxInterceptor.class), false);
         }
         else {
            interceptorChain.appendInterceptor(createInterceptor(new L1NonTxInterceptor(), L1NonTxInterceptor.class), false);
         }
      }

      if (configuration.sites().hasAsyncEnabledBackups() && cacheMode.isClustered()) {
         if (transactionMode == TransactionMode.TRANSACTIONAL) {
            if (configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
               interceptorChain.appendInterceptor(createInterceptor(new OptimisticTxIracLocalSiteInterceptor(), OptimisticTxIracLocalSiteInterceptor.class), false);
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new PessimisticTxIracLocalInterceptor(), PessimisticTxIracLocalInterceptor.class), false);
            }
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new NonTxIracLocalSiteInterceptor(), NonTxIracLocalSiteInterceptor.class), false);
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
               interceptorChain.appendInterceptor(createInterceptor(new VersionedDistributionInterceptor(), VersionedDistributionInterceptor.class), false);
               break;
            }
         case DIST_ASYNC:
         case REPL_ASYNC:
            if (transactionMode.isTransactional()) {
               interceptorChain.appendInterceptor(createInterceptor(new TxDistributionInterceptor(), TxDistributionInterceptor.class), false);
            } else {
               if (cacheMode.isDistributed() && Configurations.isEmbeddedMode(globalConfiguration)) {
                  interceptorChain.appendInterceptor(createInterceptor(new TriangleDistributionInterceptor(), TriangleDistributionInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new NonTxDistributionInterceptor(), NonTxDistributionInterceptor.class), false);
               }
            }
            break;
         case LOCAL:
            //Nothing...
      }

      if (cacheMode.isClustered()) {
         //local caches not involved in Cross Site Replication
         interceptorChain.appendInterceptor(
               createInterceptor(new NonTxIracRemoteSiteInterceptor(needsVersionAwareComponents), NonTxIracRemoteSiteInterceptor.class), false);
      }

      AsyncInterceptor callInterceptor = createInterceptor(new CallInterceptor(), CallInterceptor.class);
      interceptorChain.appendInterceptor(callInterceptor, false);
      log.trace("Finished building default interceptor chain.");
      buildCustomInterceptors(interceptorChain, configuration.customInterceptors());
      return interceptorChain;
   }

   private Class<? extends AsyncInterceptor> addInterceptor(AsyncInterceptorChain interceptorChain, AsyncInterceptor interceptor,
                                                            Class<? extends AsyncInterceptor> interceptorClass, Class<? extends AsyncInterceptor> after) {

      if (interceptorChain.containsInterceptorType(interceptorClass, true)) {
         return after;
      }

      AsyncInterceptor newInterceptor = createInterceptor(interceptor, interceptorClass);
      boolean added = interceptorChain.addInterceptorAfter(newInterceptor, after);

      return added ? newInterceptor.getClass() : after;
   }

   /**
    * Adds all the interceptors related to persistence to the stack.
    *
    * @param interceptorChain The chain
    * @param cacheConfiguration The configuration of the cache that owns the interceptor
    * @param stores A list of {@link StoreConfiguration} possibly not present in the cacheConfiguration
    */
   public void addPersistenceInterceptors(AsyncInterceptorChain interceptorChain, Configuration cacheConfiguration, List<StoreConfiguration> stores) {
      TransactionMode transactionMode = cacheConfiguration.transaction().transactionMode();
      CacheMode cacheMode = cacheConfiguration.clustering().cacheMode();

      EntryWrappingInterceptor ewi = interceptorChain.findInterceptorExtending(EntryWrappingInterceptor.class);
      if (ewi == null) {
         throw new CacheException("Cannot find instance of EntryWrappingInterceptor in the interceptor chain");
      }

      Class<? extends AsyncInterceptor> lastAdded = ewi.getClass();
      if (cacheConfiguration.persistence().passivation()) {
         if (cacheMode.isClustered()) {
            lastAdded = addInterceptor(interceptorChain, new PassivationClusteredCacheLoaderInterceptor<>(), CacheLoaderInterceptor.class, lastAdded);
         } else {
            lastAdded = addInterceptor(interceptorChain, new PassivationCacheLoaderInterceptor<>(), CacheLoaderInterceptor.class, lastAdded);
         }
         addInterceptor(interceptorChain, new PassivationWriterInterceptor(), PassivationWriterInterceptor.class, lastAdded);
      } else {
         if (cacheMode.isClustered()) {
            lastAdded = addInterceptor(interceptorChain, new ClusteredCacheLoaderInterceptor<>(), CacheLoaderInterceptor.class, lastAdded);
         } else {
            lastAdded = addInterceptor(interceptorChain, new CacheLoaderInterceptor<>(), CacheLoaderInterceptor.class, lastAdded);
         }
         boolean transactionalStore = cacheConfiguration.persistence().stores().stream().anyMatch(StoreConfiguration::transactional) ||
               stores.stream().anyMatch(StoreConfiguration::transactional);
         if (transactionalStore && transactionMode.isTransactional())
            lastAdded = addInterceptor(interceptorChain, new TransactionalStoreInterceptor(), TransactionalStoreInterceptor.class, lastAdded);

         switch (cacheMode) {
            case DIST_SYNC:
            case DIST_ASYNC:
            case REPL_SYNC:
            case REPL_ASYNC:
               addInterceptor(interceptorChain, new DistCacheWriterInterceptor(), DistCacheWriterInterceptor.class, lastAdded);
               break;
            default:
               addInterceptor(interceptorChain, new CacheWriterInterceptor(), CacheWriterInterceptor.class, lastAdded);
               break;
         }
      }
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
   public Object construct(String componentName) {
      try {
         if (configuration.simpleCache())
            return EmptyAsyncInterceptorChain.INSTANCE;

         return buildInterceptorChain();
      } catch (CacheException ce) {
         throw ce;
      } catch (Exception e) {
         throw new CacheConfigurationException("Unable to build interceptor chain", e);
      }
   }

   /**
    * @deprecated Since 9.4, not used.
    */
   @Deprecated
   public static InterceptorChainFactory getInstance(ComponentRegistry componentRegistry, Configuration configuration) {
      InterceptorChainFactory icf = new InterceptorChainFactory();
      icf.componentRegistry = componentRegistry;
      icf.configuration = configuration;
      return icf;
   }
}
