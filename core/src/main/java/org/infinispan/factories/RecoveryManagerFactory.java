/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Factory for RecoveryManager.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {RecoveryManager.class})
public class RecoveryManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final long DEFAULT_EXPIRY = TimeUnit.HOURS.toMillis(6);

   @Override
   @SuppressWarnings("unchecked")
   public <RecoveryManager> RecoveryManager construct(Class<RecoveryManager> componentType) {
      checkAsyncCache(configuration);
      boolean recoveryEnabled = configuration.isTransactionRecoveryEnabled();
      String cacheName = configuration.getName() == null ? CacheContainer.DEFAULT_CACHE_NAME : configuration.getName();
      if (recoveryEnabled) {
         String recoveryCacheName = configuration.getTransactionRecoveryCacheName();
         if (log.isTraceEnabled()) log.trace("Using recovery cache name %s", recoveryCacheName);
         EmbeddedCacheManager cm = componentRegistry.getGlobalComponentRegistry().getComponent(EmbeddedCacheManager.class);
         boolean useDefaultCache = recoveryCacheName.equals(Configuration.RecoveryType.DEFAULT_RECOVERY_INFO_CACHE);

         //if use a defined cache
         if (!useDefaultCache) {
            // check to see that the cache is defined
            if (!cm.getCacheNames().contains(recoveryCacheName)) {
               throw new ConfigurationException("Recovery cache (" + recoveryCacheName + ") does not exist!!");
            }
          } else {
            //this might have already been defined by other caches
            if (!cm.getCacheNames().contains(recoveryCacheName)) {
               Configuration config = getDefaultRecoveryCacheConfig();
               cm.defineConfiguration(recoveryCacheName, config);
            }
         }
         return (RecoveryManager) withRecoveryCache(cacheName, recoveryCacheName, cm);
      } else {
         return null;
      }
   }

   private void checkAsyncCache(Configuration configuration) {
      if (configuration.isTransactionRecoveryEnabled() && configuration.isOnePhaseCommit()) {
         throw new ConfigurationException("Recovery for async caches is not supported!");
      }
   }

   private Configuration getDefaultRecoveryCacheConfig() {
      Configuration config = new Configuration();
      config.fluent().clustering().mode(Configuration.CacheMode.LOCAL);
      config.fluent().expiration().lifespan(DEFAULT_EXPIRY);
      config.fluent().recovery().disable();
      return config;
   }

   private RecoveryManager withRecoveryCache(String cacheName, String recoveryCacheName, EmbeddedCacheManager cm) {
      Cache recoveryCache = cm.getCache(recoveryCacheName);
      return new RecoveryManagerImpl(recoveryCache,  cacheName);
   }
}
