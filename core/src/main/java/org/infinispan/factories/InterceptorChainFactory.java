package org.infinispan.factories;


import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.*;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.*;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.compat.TypeConverterInterceptor;
import org.infinispan.interceptors.distribution.*;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.*;
import org.infinispan.interceptors.xsite.NonTransactionalBackupInterceptor;
import org.infinispan.interceptors.xsite.OptimisticBackupInterceptor;
import org.infinispan.interceptors.xsite.PessimisticBackupInterceptor;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.statetransfer.TransactionSynchronizerInterceptor;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

import static org.infinispan.commons.util.ReflectionUtil.applyProperties;

/**
 * Factory class that builds an interceptor chain based on cache configuration.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @author Marko Luksa
 * @author Pedro Ruivo
 * @since 4.0
 */
@DefaultFactoryFor(classes = InterceptorChain.class)
public class InterceptorChainFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(InterceptorChainFactory.class);

   private CommandInterceptor createInterceptor(CommandInterceptor interceptor, Class<? extends CommandInterceptor> interceptorType) {
      CommandInterceptor chainedInterceptor = componentRegistry.getComponent(interceptorType);
      if (chainedInterceptor == null) {
         chainedInterceptor = interceptor;
         register(interceptorType, chainedInterceptor);
      } else {
         // wipe next/last chaining!!
         chainedInterceptor.setNext(null);
      }
      return chainedInterceptor;
   }


   private void register(Class<? extends CommandInterceptor> clazz, CommandInterceptor chainedInterceptor) {
      try {
         componentRegistry.registerComponent(chainedInterceptor, clazz);
      } catch (RuntimeException e) {
         log.unableToCreateInterceptor(clazz, e);
         throw e;
      }
   }

   private boolean isUsingMarshalledValues(Configuration c) {
      return c.storeAsBinary().enabled() && (c.storeAsBinary().storeKeysAsBinary() || c.storeAsBinary().storeValuesAsBinary());
   }

   public InterceptorChain buildInterceptorChain() {
      boolean needsVersionAwareComponents = configuration.transaction().transactionMode().isTransactional() &&
            Configurations.isVersioningEnabled(configuration);

      InterceptorChain interceptorChain = new InterceptorChain(componentRegistry.getComponentMetadataRepo());
      // add the interceptor chain to the registry first, since some interceptors may ask for it.
      componentRegistry.registerComponent(interceptorChain, InterceptorChain.class);

      boolean invocationBatching = configuration.invocationBatching().enabled();
      boolean isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();

      // load the icInterceptor first
      if (invocationBatching) {
         interceptorChain.setFirstInChain(createInterceptor(new BatchingInterceptor(), BatchingInterceptor.class));
      } else {
         interceptorChain.setFirstInChain(createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class));
      }


      CompatibilityModeConfiguration compatibility = configuration.compatibility();
      if (compatibility.enabled()) {
         Marshaller compatibilityMarshaller = compatibility.marshaller();
         if (compatibilityMarshaller != null) {
            componentRegistry.wireDependencies(compatibilityMarshaller);
         }
         interceptorChain.appendInterceptor(createInterceptor(
               new TypeConverterInterceptor(compatibilityMarshaller), TypeConverterInterceptor.class), false);
      }

      // add marshallable check interceptor for situations where we want to figure out before marshalling
      // Store as binary marshalls keys/values eagerly now, so avoid extra serialization
      if (configuration.clustering().async().asyncMarshalling()
            || configuration.clustering().async().useReplQueue() || hasAsyncStore())
         interceptorChain.appendInterceptor(createInterceptor(new IsMarshallableInterceptor(), IsMarshallableInterceptor.class), false);

      // NOW add the ICI if we are using batching!
      if (invocationBatching) {
         interceptorChain.appendInterceptor(createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class), false);
      }

      // load the cache management interceptor next
      interceptorChain.appendInterceptor(createInterceptor(new CacheMgmtInterceptor(), CacheMgmtInterceptor.class), false);

      // load the state transfer lock interceptor
      // the state transfer lock ensures that the cache member list is up-to-date
      // so it's necessary even if state transfer is disabled
      if (configuration.clustering().cacheMode().isDistributed() || configuration.clustering().cacheMode().isReplicated()) {
         if (isTotalOrder) {
            interceptorChain.appendInterceptor(createInterceptor(new TotalOrderStateTransferInterceptor(),
                                                                 TotalOrderStateTransferInterceptor.class), false);
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new StateTransferInterceptor(), StateTransferInterceptor.class), false);
         }
         interceptorChain.appendInterceptor(createInterceptor(new TransactionSynchronizerInterceptor(), TransactionSynchronizerInterceptor.class), false);
      }

      //load total order interceptor
      if (isTotalOrder) {
         interceptorChain.appendInterceptor(createInterceptor(new TotalOrderInterceptor(), TotalOrderInterceptor.class), false);
      }

      // load the tx interceptor
      if (configuration.transaction().transactionMode().isTransactional())
         interceptorChain.appendInterceptor(createInterceptor(new TxInterceptor(), TxInterceptor.class), false);

      if (isUsingMarshalledValues(configuration)) {
         CommandInterceptor interceptor;
            interceptor = createInterceptor(
                  new MarshalledValueInterceptor(), MarshalledValueInterceptor.class);

         interceptorChain.appendInterceptor(interceptor, false);
      }


      interceptorChain.appendInterceptor(createInterceptor(new NotificationInterceptor(), NotificationInterceptor.class), false);

      if (configuration.transaction().useEagerLocking()) {
         configuration.transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      //the total order protocol doesn't need locks
      if (!isTotalOrder) {
         if (configuration.transaction().transactionMode().isTransactional()) {
            if (configuration.transaction().lockingMode() == LockingMode.PESSIMISTIC) {
               interceptorChain.appendInterceptor(createInterceptor(new PessimisticLockingInterceptor(), PessimisticLockingInterceptor.class), false);
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new OptimisticLockingInterceptor(), OptimisticLockingInterceptor.class), false);
            }
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new NonTransactionalLockingInterceptor(), NonTransactionalLockingInterceptor.class), false);
         }
      }

      if (configuration.sites().hasEnabledBackups() && !configuration.sites().disableBackups()) {
         if ((configuration.transaction().transactionMode() == TransactionMode.TRANSACTIONAL)) {
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

      if (needsVersionAwareComponents && configuration.clustering().cacheMode().isClustered()) {
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
            if (configuration.clustering().cacheMode().isClustered())
               interceptorChain.appendInterceptor(createInterceptor(new ClusteredActivationInterceptor(), ClusteredActivationInterceptor.class), false);
            else
               interceptorChain.appendInterceptor(createInterceptor(new ActivationInterceptor(), ActivationInterceptor.class), false);
            interceptorChain.appendInterceptor(createInterceptor(new PassivationInterceptor(), PassivationInterceptor.class), false);
         } else {
            if (configuration.clustering().cacheMode().isClustered())
               interceptorChain.appendInterceptor(createInterceptor(new ClusteredCacheLoaderInterceptor(), ClusteredCacheLoaderInterceptor.class), false);
            else
               interceptorChain.appendInterceptor(createInterceptor(new CacheLoaderInterceptor(), CacheLoaderInterceptor.class), false);
            switch (configuration.clustering().cacheMode()) {
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
         if (configuration.transaction().transactionMode().isTransactional()) {
            interceptorChain.appendInterceptor(createInterceptor(new L1TxInterceptor(), L1TxInterceptor.class), false);
         }
         else {
            interceptorChain.appendInterceptor(createInterceptor(new L1NonTxInterceptor(), L1NonTxInterceptor.class), false);
         }
      }

      switch (configuration.clustering().cacheMode()) {
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
            if (configuration.transaction().transactionMode().isTransactional()) {
               if (isTotalOrder) {
                  interceptorChain.appendInterceptor(createInterceptor(new TotalOrderDistributionInterceptor(), TotalOrderDistributionInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new TxDistributionInterceptor(), TxDistributionInterceptor.class), false);
               }
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new NonTxDistributionInterceptor(), NonTxDistributionInterceptor.class), false);
            }
            break;
         case LOCAL:
            //Nothing...
      }

      CommandInterceptor callInterceptor = createInterceptor(new CallInterceptor(), CallInterceptor.class);
      interceptorChain.appendInterceptor(callInterceptor, false);
      log.trace("Finished building default interceptor chain.");
      buildCustomInterceptors(interceptorChain, configuration.customInterceptors());
      return interceptorChain;
   }

   private void buildCustomInterceptors(InterceptorChain interceptorChain, CustomInterceptorsConfiguration customInterceptors) {
      for (InterceptorConfiguration config : customInterceptors.interceptors()) {
         if (interceptorChain.containsInterceptorType(config.interceptor().getClass())) continue;

         CommandInterceptor customInterceptor = config.interceptor();
         applyProperties(customInterceptor, config.properties());
         register(customInterceptor.getClass(), customInterceptor);
         if (config.first())
            interceptorChain.addInterceptor(customInterceptor, 0);
         else if (config.last())
            interceptorChain.appendInterceptor(customInterceptor, true);
         else if (config.index() >= 0)
            interceptorChain.addInterceptor(customInterceptor, config.index());
         else if (config.after() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClass(config.after());
            if (withClassName.isEmpty()) {
               throw new CacheConfigurationException("Cannot add after class: " + config.after()
                                                      + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorAfter(customInterceptor, withClassName.get(0).getClass());
         } else if (config.before() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClass(config.before());
            if (withClassName.isEmpty()) {
               throw new CacheConfigurationException("Cannot add before class: " + config.after()
                                                      + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorBefore(customInterceptor, withClassName.get(0).getClass());
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
         return componentType.cast(buildInterceptorChain());
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
