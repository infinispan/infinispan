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
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.RecoveryConfiguration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryInfoKey;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Factory for RecoveryManager.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@DefaultFactoryFor(classes = {RecoveryManager.class})
public class RecoveryManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(RecoveryManagerFactory.class);

   private static final long DEFAULT_EXPIRY = TimeUnit.HOURS.toMillis(6);

   @Override
   @SuppressWarnings("unchecked")
   public <RecoveryManager> RecoveryManager construct(Class<RecoveryManager> componentType) {
      boolean recoveryEnabled = configuration.transaction().recovery().enabled()
            && !configuration.transaction().useSynchronization();
      if (recoveryEnabled) {
         String recoveryCacheName = configuration.transaction().recovery().recoveryInfoCacheName();
         log.tracef("Using recovery cache name %s", recoveryCacheName);
         EmbeddedCacheManager cm = componentRegistry.getGlobalComponentRegistry().getComponent(EmbeddedCacheManager.class);
         boolean useDefaultCache = recoveryCacheName.equals(RecoveryConfiguration.DEFAULT_RECOVERY_INFO_CACHE);

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
         return (RecoveryManager) buildRecoveryManager(componentRegistry.getCacheName(),
               recoveryCacheName, cm, useDefaultCache);
      } else {
         return null;
      }
   }

   private Configuration getDefaultRecoveryCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //the recovery cache should not participate in main cache's transactions, especially because removals
      // from this cache are executed in the context of a finalised transaction and cause issues.
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      builder.clustering().cacheMode(CacheMode.LOCAL);
      builder.expiration().lifespan(DEFAULT_EXPIRY);
      builder.transaction().recovery().disable();
      return builder.build();
   }

   private RecoveryManager buildRecoveryManager(String cacheName, String recoveryCacheName, EmbeddedCacheManager cm, boolean isDefault) {
      log.tracef("About to obtain a reference to the recovery cache: %s", recoveryCacheName);
      Cache<?, ?> recoveryCache = cm.getCache(recoveryCacheName);
      if (recoveryCache.getConfiguration().isTransactionalCache()) {
         //see comment in getDefaultRecoveryCacheConfig
         throw new ConfigurationException("The recovery cache shouldn't be transactional.");
      }
      log.tracef("Obtained a reference to the recovery cache: %s", recoveryCacheName);
      return new RecoveryManagerImpl((ConcurrentMap<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryCache,  cacheName);
   }
}
