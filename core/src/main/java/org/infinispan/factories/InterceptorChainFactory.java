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
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.*;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TOVersionedEntryWrappingInterceptor;
import org.infinispan.interceptors.totalorder.TOVersionedReplicationInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

/**
 * Factory class that builds an interceptor chain based on cache configuration.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
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
      return c.isStoreAsBinary() && (c.isStoreKeysAsBinary() || c.isStoreValuesAsBinary());
   }

   public InterceptorChain buildInterceptorChain() {
      boolean needsVersionAwareComponents = configuration.isTransactionalCache() && configuration.isWriteSkewCheck() &&
            configuration.getTransactionLockingMode() == LockingMode.OPTIMISTIC && configuration.isEnableVersioning();

      boolean invocationBatching = configuration.isInvocationBatchingEnabled();
      // load the icInterceptor first

      CommandInterceptor first;
      if (invocationBatching) {
         first = createInterceptor(new BatchingInterceptor(), BatchingInterceptor.class);
      } else {
         first = createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class);
      }

      InterceptorChain interceptorChain = new InterceptorChain(first);

      // add the interceptor chain to the registry first, since some interceptors may ask for it.
      componentRegistry.registerComponent(interceptorChain, InterceptorChain.class);

      // add marshallable check interceptor for situations where we want to figure out before marshalling
      if (isUsingMarshalledValues(configuration) || configuration.isUseAsyncMarshalling()
            || configuration.isUseReplQueue() || hasAsyncStore())
         interceptorChain.appendInterceptor(createInterceptor(new IsMarshallableInterceptor(), IsMarshallableInterceptor.class), false);

      // NOW add the ICI if we are using batching!
      if (invocationBatching) {
         interceptorChain.appendInterceptor(createInterceptor(new InvocationContextInterceptor(), InvocationContextInterceptor.class), false);
      }

      // load the cache management interceptor next
      if (configuration.isExposeJmxStatistics())
         interceptorChain.appendInterceptor(createInterceptor(new CacheMgmtInterceptor(), CacheMgmtInterceptor.class), false);

      // load the state transfer lock interceptor
      // the state transfer lock ensures that the cache member list is up-to-date
      // so it's necessary even if state transfer is disabled
      if (configuration.getCacheMode().isDistributed() || configuration.getCacheMode().isReplicated())
         interceptorChain.appendInterceptor(createInterceptor(new StateTransferLockInterceptor(), StateTransferLockInterceptor.class), false);

      //load total order interceptor
      if (configuration.isTotalOrder()) {
         interceptorChain.appendInterceptor(createInterceptor(new TotalOrderInterceptor(), TotalOrderInterceptor.class),
               false);
      }

      // load the tx interceptor
      if (configuration.isTransactionalCache())
         interceptorChain.appendInterceptor(createInterceptor(new TxInterceptor(), TxInterceptor.class), false);

      if (isUsingMarshalledValues(configuration))
         interceptorChain.appendInterceptor(createInterceptor(new MarshalledValueInterceptor(), MarshalledValueInterceptor.class), false);

      interceptorChain.appendInterceptor(createInterceptor(new NotificationInterceptor(), NotificationInterceptor.class), false);

      if (configuration.isUseEagerLocking()) {
         configuration.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      //the total order protocol doesn't need locks
      if (!configuration.isTotalOrder()) {
         if (configuration.isTransactionalCache()) {
            if (configuration.getTransactionLockingMode() == LockingMode.PESSIMISTIC) {
               interceptorChain.appendInterceptor(createInterceptor(new PessimisticLockingInterceptor(), PessimisticLockingInterceptor.class), false);
            } else {
               interceptorChain.appendInterceptor(createInterceptor(new OptimisticLockingInterceptor(), OptimisticLockingInterceptor.class), false);
            }
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new NonTransactionalLockingInterceptor(), NonTransactionalLockingInterceptor.class), false);
         }
      }

      if (needsVersionAwareComponents && configuration.getCacheMode().isClustered()) {
         //added custom entry wrapping interceptor for total order protocol
         if (configuration.isTotalOrder()) {
            interceptorChain.appendInterceptor(createInterceptor(new TOVersionedEntryWrappingInterceptor(),
                  TOVersionedEntryWrappingInterceptor.class), false);
         } else {
            interceptorChain.appendInterceptor(createInterceptor(new VersionedEntryWrappingInterceptor(), VersionedEntryWrappingInterceptor.class), false);
         }
      } else
         interceptorChain.appendInterceptor(createInterceptor(new EntryWrappingInterceptor(), EntryWrappingInterceptor.class), false);

      if (configuration.isUsingCacheLoaders()) {
         if (configuration.getCacheLoaderManagerConfig().isPassivation()) {
            if (configuration.getCacheMode().isClustered())
               interceptorChain.appendInterceptor(createInterceptor(new ClusteredActivationInterceptor(), ClusteredActivationInterceptor.class), false);
            else
               interceptorChain.appendInterceptor(createInterceptor(new ActivationInterceptor(), ActivationInterceptor.class), false);
            interceptorChain.appendInterceptor(createInterceptor(new PassivationInterceptor(), PassivationInterceptor.class), false);
         } else {
            if (configuration.getCacheMode().isClustered())
               interceptorChain.appendInterceptor(createInterceptor(new ClusteredCacheLoaderInterceptor(), ClusteredCacheLoaderInterceptor.class), false);
            else
               interceptorChain.appendInterceptor(createInterceptor(new CacheLoaderInterceptor(), CacheLoaderInterceptor.class), false);
            switch (configuration.getCacheMode()) {
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

      //total order protocol has no locks, then it has no deadlocks
      if (configuration.isEnableDeadlockDetection() && !configuration.isTotalOrder()) {
         interceptorChain.appendInterceptor(createInterceptor(new DeadlockDetectingInterceptor(), DeadlockDetectingInterceptor.class), false);
      }

      switch (configuration.getCacheMode()) {
         case REPL_SYNC:
            if (needsVersionAwareComponents) {
               //added custom interceptor to replace the original
               if (configuration.isTotalOrder()) {
                  interceptorChain.appendInterceptor(createInterceptor(new TOVersionedReplicationInterceptor(),
                        TOVersionedReplicationInterceptor.class), false);
               } else {
                  interceptorChain.appendInterceptor(createInterceptor(new VersionedReplicationInterceptor(), VersionedReplicationInterceptor.class), false);
               }
               break;
            }
         case REPL_ASYNC:
            interceptorChain.appendInterceptor(createInterceptor(new ReplicationInterceptor(), ReplicationInterceptor.class), false);
            break;
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            interceptorChain.appendInterceptor(createInterceptor(new InvalidationInterceptor(), InvalidationInterceptor.class), false);
            break;
         case DIST_SYNC:
            if (needsVersionAwareComponents) {
               interceptorChain.appendInterceptor(createInterceptor(new VersionedDistributionInterceptor(), VersionedDistributionInterceptor.class), false);
               break;
            }
         case DIST_ASYNC:
            interceptorChain.appendInterceptor(createInterceptor(new DistributionInterceptor(), DistributionInterceptor.class), false);
            break;
         case LOCAL:
            //Nothing...
      }

      CommandInterceptor callInterceptor = createInterceptor(new CallInterceptor(), CallInterceptor.class);
      interceptorChain.appendInterceptor(callInterceptor, false);
      log.trace("Finished building default interceptor chain.");
      buildCustomInterceptors(interceptorChain, configuration.getCustomInterceptors());
      return interceptorChain;
   }

   @SuppressWarnings("unchecked")
   private Class<? extends CommandInterceptor> getCustomInterceptorType(CustomInterceptorConfig cfg) {
      if (cfg.getInterceptor() != null) return cfg.getInterceptor().getClass();
      return Util.loadClass(cfg.getClassName(), configuration.getClassLoader());
   }

   private CommandInterceptor getOrCreateCustomInterceptor(CustomInterceptorConfig cfg) {
      CommandInterceptor result = cfg.getInterceptor();
      if (result == null) {
         result = Util.getInstance(cfg.getClassName(), configuration.getClassLoader());
      }
      register(result.getClass(), result);
      return result;
   }

   private void buildCustomInterceptors(InterceptorChain interceptorChain, List<CustomInterceptorConfig> customInterceptors) {
      for (CustomInterceptorConfig config : customInterceptors) {
         if (interceptorChain.containsInterceptorType(getCustomInterceptorType(config))) continue;

         CommandInterceptor customInterceptor = getOrCreateCustomInterceptor(config);
         if (config.isFirst())
            interceptorChain.addInterceptor(customInterceptor, 0);
         else if (config.isLast())
            interceptorChain.appendInterceptor(customInterceptor, true);
         else if (config.getIndex() >= 0)
            interceptorChain.addInterceptor(customInterceptor, config.getIndex());
         else if (config.getAfter() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getAfter());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add after class: " + config.getAfter()
                                                      + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorAfter(customInterceptor, withClassName.get(0).getClass());
         } else if (config.getBefore() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getBefore());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add before class: " + config.getAfter()
                                                      + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorBefore(customInterceptor, withClassName.get(0).getClass());
         }
      }

   }

   private boolean hasAsyncStore() {
      List<CacheLoaderConfig> loaderConfigs = configuration.getCacheLoaderManagerConfig().getCacheLoaderConfigs();
      for (CacheLoaderConfig loaderConfig : loaderConfigs) {
         if (loaderConfig instanceof CacheStoreConfig) {
            CacheStoreConfig storeConfig = (CacheStoreConfig) loaderConfig;
            if (storeConfig.getAsyncStoreConfig().isEnabled())
               return true;
         }
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
