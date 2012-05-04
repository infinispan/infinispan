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
package org.infinispan.query.impl;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.hibernate.search.cfg.spi.SearchConfiguration;
import org.hibernate.search.spi.SearchFactoryBuilder;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.query.CommandInitializer;
import org.infinispan.query.backend.LocalQueryInterceptor;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.backend.SearchableCacheConfiguration;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.logging.Log;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.LogFactory;

/**
 * Lifecycle of the Query module: initializes the Hibernate Search engine and shuts it down
 * at cache stop.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class LifecycleManager extends AbstractModuleLifecycle {
   
   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);
   
   private final Map<String,SearchFactoryIntegrator> searchFactoriesToShutdown = new TreeMap<String,SearchFactoryIntegrator>();

   /**
    * Registers the Search interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      Configuration cfg = cr.getComponent(Configuration.class);
      if (cfg.indexing().enabled()) {
         log.registeringQueryInterceptor();
         SearchFactoryIntegrator searchFactory = getSearchFactory(cfg.indexing().properties(), cr);
         createQueryInterceptorIfNeeded(cr, cfg, searchFactory);
      }
   }
   
   private void createQueryInterceptorIfNeeded(ComponentRegistry cr, Configuration cfg, SearchFactoryIntegrator searchFactory) {
      QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
      if (queryInterceptor == null) {
         queryInterceptor = buildQueryInterceptor(cfg, searchFactory);
         cr.registerComponent(queryInterceptor, QueryInterceptor.class);

         ConfigurationBuilder builder = new ConfigurationBuilder();
         InterceptorConfigurationBuilder interceptorBuilder =
               builder.customInterceptors().addInterceptor();
         interceptorBuilder.interceptor(queryInterceptor);

         if (!cfg.transaction().transactionMode().isTransactional()) {
            interceptorBuilder.after(NonTransactionalLockingInterceptor.class);
         } else if (cfg.transaction().lockingMode() == LockingMode.OPTIMISTIC) {
            interceptorBuilder.after(OptimisticLockingInterceptor.class);
         } else {
            interceptorBuilder.after(PessimisticLockingInterceptor.class);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   private QueryInterceptor buildQueryInterceptor(Configuration cfg, SearchFactoryIntegrator searchFactory) {
      if ( cfg.indexing().indexLocalOnly() ) {
         return new LocalQueryInterceptor(searchFactory);
      }
      else {
         return new QueryInterceptor(searchFactory);
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      boolean indexingEnabled = configuration.indexing().enabled();
      if ( ! indexingEnabled ) {
         if ( verifyChainContainsQueryInterceptor(cr) ) {
            throw new IllegalStateException( "It was NOT expected to find the Query interceptor registered in the InterceptorChain as indexing was disabled, but it was found" );
         }
         return;
      }
      if ( ! verifyChainContainsQueryInterceptor(cr) ) {
         throw new IllegalStateException( "It was expected to find the Query interceptor registered in the InterceptorChain but it wasn't found" );
      }
      
      // initializing the query module command initializer. we can t inject Cache with @inject in there
      Cache<?, ?> cache = cr.getComponent(Cache.class);
      CommandInitializer initializer = cr.getComponent(CommandInitializer.class);
      initializer.setCache(cache);
      
      QueryBox queryBox = new QueryBox();
      queryBox.setCache(cache.getAdvancedCache());
      cr.registerComponent(queryBox, QueryBox.class);
   }

   private boolean verifyChainContainsQueryInterceptor(ComponentRegistry cr) {
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      CommandInterceptor chainElement = interceptorChain.getFirstInChain();
      if (chainElement instanceof QueryInterceptor) {
         return true;
      }
      while (chainElement.hasNext()) {
         chainElement = chainElement.getNext();
         if (chainElement instanceof QueryInterceptor) {
            return true;
         }
      }
      return false;
   }

   private SearchFactoryIntegrator getSearchFactory(Properties indexingProperties, ComponentRegistry cr) {
       SearchFactoryIntegrator searchFactory = cr.getComponent(SearchFactoryIntegrator.class);
       //defend against multiple initialization:
       if (searchFactory==null) {
          // Set up the search factory for Hibernate Search first.
          SearchConfiguration config = new SearchableCacheConfiguration(new Class[0], indexingProperties);
          searchFactory = new SearchFactoryBuilder().configuration(config).buildSearchFactory();
          cr.registerComponent(searchFactory, SearchFactoryIntegrator.class);
       }
      return searchFactory;
   }
   
   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      //TODO move this to cacheStopped event (won't work right now as the ComponentRegistry is half empty at that point: ISPN-1006)
      SearchFactoryIntegrator searchFactoryImplementor = cr.getComponent(SearchFactoryIntegrator.class);
      if (searchFactoryImplementor != null) {
         searchFactoriesToShutdown.put(cacheName, searchFactoryImplementor);
      }
   }
   
   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      SearchFactoryIntegrator searchFactoryIntegrator = searchFactoriesToShutdown.remove(cacheName);
      if (searchFactoryIntegrator != null) {
         searchFactoryIntegrator.close();
      }
   }

}
