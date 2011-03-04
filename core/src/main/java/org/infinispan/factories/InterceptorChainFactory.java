/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.util.Util;

import java.util.List;

/**
 * Factory class that builds an interceptor chain based on cache configuration.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
@DefaultFactoryFor(classes = InterceptorChain.class)
public class InterceptorChainFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   public CommandInterceptor createInterceptor(Class<? extends CommandInterceptor> clazz) {
      CommandInterceptor chainedInterceptor = componentRegistry.getComponent(clazz);
      if (chainedInterceptor == null) {
         chainedInterceptor = Util.getInstance(clazz);
         try {
            componentRegistry.registerComponent(chainedInterceptor, clazz);
         }
         catch (RuntimeException e) {
            log.warn("Problems creating interceptor " + clazz);
            throw e;
         }
      } else {
         // wipe next/last chaining!!
         chainedInterceptor.setNext(null);
      }
      return chainedInterceptor;
   }

   public InterceptorChain buildInterceptorChain() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
      boolean invocationBatching = configuration.isInvocationBatchingEnabled();
      // load the icInterceptor first

      CommandInterceptor first = invocationBatching ? createInterceptor(BatchingInterceptor.class) : createInterceptor(InvocationContextInterceptor.class);

      InterceptorChain interceptorChain = new InterceptorChain(first);

      // add the interceptor chain to the registry first, since some interceptors may ask for it.
      componentRegistry.registerComponent(interceptorChain, InterceptorChain.class);

      // add marshallable check interceptor for situations where we want to figure out before marshalling
      if (configuration.isUseLazyDeserialization() || configuration.isUseAsyncMarshalling()
            || configuration.isUseReplQueue() || hasAsyncStore())
         interceptorChain.appendIntereceptor(createInterceptor(IsMarshallableInterceptor.class));

      // NOW add the ICI if we are using batching!
      if (invocationBatching)
         interceptorChain.appendIntereceptor(createInterceptor(InvocationContextInterceptor.class));

      // load the cache management interceptor next
      if (configuration.isExposeJmxStatistics())
         interceptorChain.appendIntereceptor(createInterceptor(CacheMgmtInterceptor.class));

      // load the tx interceptor
      if (configuration.getCacheMode().isDistributed())
         interceptorChain.appendIntereceptor(createInterceptor(DistTxInterceptor.class));
      else
         interceptorChain.appendIntereceptor(createInterceptor(TxInterceptor.class));

      if(configuration.isUseEagerLocking())
         interceptorChain.appendIntereceptor(createInterceptor(ImplicitEagerLockingInterceptor.class));

      if (configuration.isUseLazyDeserialization())
         interceptorChain.appendIntereceptor(createInterceptor(MarshalledValueInterceptor.class));

      interceptorChain.appendIntereceptor(createInterceptor(NotificationInterceptor.class));

      if (configuration.isEnableDeadlockDetection()) {
         interceptorChain.appendIntereceptor(createInterceptor(DeadlockDetectingInterceptor.class));
      }

      if (configuration.isUsingCacheLoaders()) {
         if (configuration.getCacheLoaderManagerConfig().isPassivation()) {
            interceptorChain.appendIntereceptor(createInterceptor(ActivationInterceptor.class));
            interceptorChain.appendIntereceptor(createInterceptor(PassivationInterceptor.class));
         } else {
            interceptorChain.appendIntereceptor(createInterceptor(CacheLoaderInterceptor.class));
            switch (configuration.getCacheMode()) {
               case DIST_SYNC:
               case DIST_ASYNC:
                  interceptorChain.appendIntereceptor(createInterceptor(DistCacheStoreInterceptor.class));
                  break;
               default:
                  interceptorChain.appendIntereceptor(createInterceptor(CacheStoreInterceptor.class));
                  break;
            }
         }
      }


      if (configuration.getCacheMode().isDistributed())
         interceptorChain.appendIntereceptor(createInterceptor(DistLockingInterceptor.class));
      else
         interceptorChain.appendIntereceptor(createInterceptor(LockingInterceptor.class));

      switch (configuration.getCacheMode()) {
         case REPL_SYNC:
         case REPL_ASYNC:
            interceptorChain.appendIntereceptor(createInterceptor(ReplicationInterceptor.class));
            break;
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            interceptorChain.appendIntereceptor(createInterceptor(InvalidationInterceptor.class));
            break;
         case DIST_SYNC:
         case DIST_ASYNC:
            interceptorChain.appendIntereceptor(createInterceptor(DistributionInterceptor.class));
            break;
         case LOCAL:
            //Nothing...
      }

      CommandInterceptor callInterceptor = createInterceptor(CallInterceptor.class);
      interceptorChain.appendIntereceptor(callInterceptor);
      if (log.isTraceEnabled()) log.trace("Finished building default interceptor chain.");
      buildCustomInterceptors(interceptorChain, configuration.getCustomInterceptors());
      return interceptorChain;
   }

   @SuppressWarnings("unchecked")
   private Class<? extends CommandInterceptor> getCustomInterceptorType(CustomInterceptorConfig cfg) {
      if (cfg.getInterceptor() != null) return cfg.getInterceptor().getClass();
      return Util.loadClass(cfg.getClassName());
   }

   private CommandInterceptor getOrCreateCustomInterceptor(CustomInterceptorConfig cfg) {
      if (cfg.getInterceptor() != null) return cfg.getInterceptor();
      return (CommandInterceptor) Util.getInstance(cfg.getClassName());
   }

   private void buildCustomInterceptors(InterceptorChain interceptorChain, List<CustomInterceptorConfig> customInterceptors) {
      for (CustomInterceptorConfig config : customInterceptors) {
         if (interceptorChain.containsInterceptorType(getCustomInterceptorType(config))) continue;
         if (config.isFirst())
            interceptorChain.addInterceptor(getOrCreateCustomInterceptor(config), 0);
         else if (config.isLast())
            interceptorChain.appendIntereceptor(getOrCreateCustomInterceptor(config));
         else if (config.getIndex() >= 0)
            interceptorChain.addInterceptor(getOrCreateCustomInterceptor(config), config.getIndex());
         else if (config.getAfter() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getAfter());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add after class: " + config.getAfter()
                     + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorAfter(getOrCreateCustomInterceptor(config), withClassName.get(0).getClass());
         }
         else if (config.getBefore() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getBefore());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add before class: " + config.getAfter()
                     + " as no such interceptor exists in the default chain");
            }
            interceptorChain.addInterceptorBefore(getOrCreateCustomInterceptor(config), withClassName.get(0).getClass());
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
      }
      catch (Exception e) {
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
