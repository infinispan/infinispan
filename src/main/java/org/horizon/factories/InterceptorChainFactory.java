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
package org.horizon.factories;


import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.config.CustomInterceptorConfig;
import org.horizon.factories.annotations.DefaultFactoryFor;
import org.horizon.interceptors.*;
import org.horizon.interceptors.base.CommandInterceptor;

import java.util.List;

/**
 * Factory class that builds an interceptor chain based on cache configuration.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
@DefaultFactoryFor(classes = InterceptorChain.class)
public class InterceptorChainFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   private CommandInterceptor createInterceptor(Class<? extends CommandInterceptor> clazz) throws IllegalAccessException, InstantiationException {
      CommandInterceptor chainedInterceptor = componentRegistry.getComponent(clazz);
      if (chainedInterceptor == null) {
         chainedInterceptor = clazz.newInstance();
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

      // NOW add the ICI if we are using batching!
      if (invocationBatching)
         interceptorChain.appendIntereceptor(createInterceptor(InvocationContextInterceptor.class));

      // load the cache management interceptor next
      if (configuration.isExposeJmxStatistics())
         interceptorChain.appendIntereceptor(createInterceptor(CacheMgmtInterceptor.class));

      // load the tx interceptor
      interceptorChain.appendIntereceptor(createInterceptor(TxInterceptor.class));

      if (configuration.isUseLazyDeserialization())
         interceptorChain.appendIntereceptor(createInterceptor(MarshalledValueInterceptor.class));

      interceptorChain.appendIntereceptor(createInterceptor(NotificationInterceptor.class));

      switch (configuration.getCacheMode()) {
         case REPL_SYNC:
         case REPL_ASYNC:
            interceptorChain.appendIntereceptor(createInterceptor(ReplicationInterceptor.class));
            break;
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            interceptorChain.appendIntereceptor(createInterceptor(InvalidationInterceptor.class));
            break;
         case LOCAL:
            //Nothing...
      }

      if (configuration.isUsingCacheLoaders()) {
         if (configuration.getCacheLoaderManagerConfig().isPassivation()) {
            interceptorChain.appendIntereceptor(createInterceptor(ActivationInterceptor.class));
         } else {
            interceptorChain.appendIntereceptor(createInterceptor(CacheLoaderInterceptor.class));
         }
      }
      interceptorChain.appendIntereceptor(createInterceptor(LockingInterceptor.class));

      if (configuration.isUsingCacheLoaders()) {
         if (configuration.getCacheLoaderManagerConfig().isPassivation()) {

            interceptorChain.appendIntereceptor(createInterceptor(PassivationInterceptor.class));

         } else {

            interceptorChain.appendIntereceptor(createInterceptor(CacheStoreInterceptor.class));

         }
      }

      if (configuration.isUsingEviction()) {
         EvictionInterceptor evictionInterceptor = (EvictionInterceptor) createInterceptor(EvictionInterceptor.class);
         interceptorChain.appendIntereceptor(evictionInterceptor);
      }

      CommandInterceptor callInterceptor = createInterceptor(CallInterceptor.class);
      interceptorChain.appendIntereceptor(callInterceptor);
      if (log.isTraceEnabled()) log.trace("Finished building default interceptor chain.");
      buildCustomInterceptors(interceptorChain, configuration.getCustomInterceptors());
      return interceptorChain;
   }

   private void buildCustomInterceptors(InterceptorChain interceptorChain, List<CustomInterceptorConfig> customInterceptors) {
      for (CustomInterceptorConfig config : customInterceptors) {
         if (interceptorChain.containsInstance(config.getInterceptor())) continue;
         if (config.isFirst()) {
            interceptorChain.addInterceptor(config.getInterceptor(), 0);
         }
         if (config.isLast()) interceptorChain.appendIntereceptor(config.getInterceptor());
         if (config.getIndex() >= 0) interceptorChain.addInterceptor(config.getInterceptor(), config.getIndex());
         if (config.getAfter() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getAfter());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add after class: " + config.getAfter()
                     + " as no such iterceptor exists in the default chain");
            }
            interceptorChain.addInterceptorAfter(config.getInterceptor(), withClassName.get(0).getClass());
         }
         if (config.getBefore() != null) {
            List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getBefore());
            if (withClassName.isEmpty()) {
               throw new ConfigurationException("Cannot add before class: " + config.getAfter()
                     + " as no such iterceptor exists in the default chain");
            }
            interceptorChain.addInterceptorBefore(config.getInterceptor(), withClassName.get(0).getClass());
         }
      }
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      try {
         return componentType.cast(buildInterceptorChain());
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
