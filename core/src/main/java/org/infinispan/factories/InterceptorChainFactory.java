/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.factories;


import org.infinispan.CacheException;
import org.infinispan.compat.TypeConverter;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.CustomInterceptorsConfiguration;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.CacheStoreConfiguration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.*;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.compat.TypeConverterInterceptor;
import org.infinispan.interceptors.distribution.L1NonTxInterceptor;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.interceptors.distribution.VersionedDistributionInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderReplicationInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderStateTransferInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedReplicationInterceptor;
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

import static org.infinispan.util.ReflectionUtil.applyProperties;

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
         log.warn("Problems creating interceptor " + clazz);
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

      TypeConverter typeConverter = configuration.dataContainer().typeConverter();
      if (typeConverter != null) {
         interceptorChain.appendInterceptor(createInterceptor(
               new TypeConverterInterceptor(typeConverter), TypeConverterInterceptor.class), false);
      }

      // add marshallable check interceptor for situations where we want to figure out before marshalling
      if (isUsingMarshalledValues(configuration) || configuration.clustering().async().asyncMarshalling()
            || configuration.clustering().async().useReplQueue() || hasAsyncStore())
         interceptorChain.appendInterceptor(createInterceptor(new IsMarshallableInterceptor(), IsMarshallableInterceptor.class), false);

      // NOW add the ICI if we are using batching!
      if (invocationBatching) {
         interceptorChain.appendInterceptor(createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class), false);
      }

      // load the cache management interceptor next
      if (configuration.jmxStatistics().enabled())
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
         if (configuration.storeAsBinary().defensive()) {
            interceptor = createInterceptor(
                  new DefensiveMarshalledValueInterceptor(), DefensiveMarshalledValueInterceptor.class);
         } else {
            interceptor = createInterceptor(
                  new MarshalledValueInterceptor(), MarshalledValueInterceptor.class);
         }

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

      if (needsVersionAwareComponents && configuration.clustering().cacheMode().isClustered()) {
         if (isTotalOrder) {
            interceptorChain.appendInterceptor(createInterceptor(new TotalOrderVersionedEntryWrappingInterceptor(),
                                                                 TotalOrderVersionedEntryWrappingInterceptor.class), false);
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new VersionedEntryWrappingInterceptor(), VersionedEntryWrappingInterceptor.class), false);
         }
      } else
         interceptorChain.appendInterceptor(createInterceptor(new EntryWrappingInterceptor(), EntryWrappingInterceptor.class), false);

      if (configuration.loaders().usingCacheLoaders()) {
         if (configuration.loaders().passivation()) {
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
                  interceptorChain.appendInterceptor(createInterceptor(new DistCacheStoreInterceptor(), DistCacheStoreInterceptor.class), false);
                  break;
               default:
                  interceptorChain.appendInterceptor(createInterceptor(new CacheStoreInterceptor(), CacheStoreInterceptor.class), false);
                  break;
            }
         }
      }

      if (configuration.deadlockDetection().enabled() && !isTotalOrder) {
         interceptorChain.appendInterceptor(createInterceptor(new DeadlockDetectingInterceptor(), DeadlockDetectingInterceptor.class), false);
      }

      if (configuration.clustering().l1().enabled() && !configuration.transaction().transactionMode().isTransactional()) {
         interceptorChain.appendInterceptor(createInterceptor(new L1NonTxInterceptor(), L1NonTxInterceptor.class), false);
      }

      switch (configuration.clustering().cacheMode()) {
         case REPL_SYNC:
            if (needsVersionAwareComponents) {
               //added custom interceptor to replace the original
               if (isTotalOrder) {
                  interceptorChain.appendInterceptor(createInterceptor(new TotalOrderVersionedReplicationInterceptor(),
                                                                       TotalOrderVersionedReplicationInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new VersionedReplicationInterceptor(), VersionedReplicationInterceptor.class), false);
               }
               break;
            }
         case REPL_ASYNC:
            if (isTotalOrder) {
               interceptorChain.appendInterceptor(createInterceptor(new TotalOrderReplicationInterceptor(), TotalOrderReplicationInterceptor.class), false);
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new ReplicationInterceptor(), ReplicationInterceptor.class), false);
            }
            break;
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            interceptorChain.appendInterceptor(createInterceptor(new InvalidationInterceptor(), InvalidationInterceptor.class), false);
            break;
         case DIST_SYNC:
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
               throw new ConfigurationException("Cannot add after class: " + config.after()
                                                      + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorAfter(customInterceptor, withClassName.get(0).getClass());
         } else if (config.before() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClass(config.before());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add before class: " + config.after()
                                                      + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorBefore(customInterceptor, withClassName.get(0).getClass());
         }
      }

   }

   private boolean hasAsyncStore() {
      List<CacheLoaderConfiguration> loaderConfigs = configuration.loaders().cacheLoaders();
      for (CacheLoaderConfiguration loaderConfig : loaderConfigs) {
         if (loaderConfig instanceof CacheStoreConfiguration && ((CacheStoreConfiguration)loaderConfig).async().enabled())
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
         throw new ConfigurationException("Unable to build interceptor chain", e);
      }
   }

   public static InterceptorChainFactory getInstance(ComponentRegistry componentRegistry, Configuration configuration) {
      InterceptorChainFactory icf = new InterceptorChainFactory();
      icf.componentRegistry = componentRegistry;
      icf.configuration = configuration;
      return icf;
   }
}
