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
package org.infinispan.query;

import org.apache.lucene.search.Query;
import org.hibernate.search.SearchFactory;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.CacheQueryImpl;

/**
 * Class that is used to build {@link org.infinispan.query.CacheQuery}
 *
 * @author Navin Surtani
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
class SearchManagerImpl implements SearchManager {

   private final AdvancedCache<?, ?> cache;
   private final SearchFactoryIntegrator searchFactory;
   private final QueryInterceptor queryInterceptor;
   
   SearchManagerImpl(AdvancedCache<?, ?> cache) {
      if (cache==null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      this.cache = cache;
      this.searchFactory = extractType(cache, SearchFactoryIntegrator.class);
      this.queryInterceptor = extractType(cache, QueryInterceptor.class);
   }

   private static <T> T extractType(Cache cache, Class<T> class1) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      T component = componentRegistry.getComponent(class1);
      if (component==null) {
         throw new IllegalArgumentException("Indexing was not enabled on this cache. " + class1 + " not found in registry");
      }
      return component;
   }

   /* (non-Javadoc)
    * @see org.infinispan.query.SearchManager#getQuery(org.apache.lucene.search.Query, java.lang.Class)
    */
   @Override
   public CacheQuery getQuery(Query luceneQuery, Class<?>... classes) {
      queryInterceptor.enableClasses(classes);
      return new CacheQueryImpl(luceneQuery, searchFactory, cache, classes);
   }

   /* (non-Javadoc)
    * @see org.infinispan.query.SearchManager#buildQueryBuilderForClass(java.lang.Class)
    */
   @Override
   public EntityContext buildQueryBuilderForClass(Class<?> entityType) {
      queryInterceptor.enableClasses(new Class[] { entityType });
      return searchFactory.buildQueryBuilder().forEntity(entityType);
   }
   
   /* (non-Javadoc)
    * @see org.infinispan.query.SearchManager#getSearchFactory()
    */
   @Override
   public SearchFactory getSearchFactory() {
      return searchFactory;
   }
   
}
