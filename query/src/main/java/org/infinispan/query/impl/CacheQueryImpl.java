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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.hibernate.search.FullTextFilter;
import org.hibernate.search.SearchException;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.query.engine.spi.FacetManager;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * Implementation class of the CacheQuery interface.
 * <p/>
 *
 * @author Navin Surtani
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public class CacheQueryImpl implements CacheQuery {

   protected final AdvancedCache<?, ?> cache;
   protected final KeyTransformationHandler keyTransformationHandler;
   protected HSQuery hSearchQuery;

   public CacheQueryImpl(Query luceneQuery, SearchFactoryIntegrator searchFactory, AdvancedCache<?, ?> cache,
         KeyTransformationHandler keyTransformationHandler, Class<?>... classes) {
      this.keyTransformationHandler = keyTransformationHandler;
      this.cache = cache;
      hSearchQuery = searchFactory.createHSQuery();
      hSearchQuery
         .luceneQuery( luceneQuery )
         //      .timeoutExceptionFactory( exceptionFactory ) Make one if needed
         .targetedEntities( Arrays.asList( classes ) );
   }

   /**
    * Takes in a lucene filter and sets it to the filter field in the class.
    *
    * @param filter - lucene filter
    */
   @Override
   public CacheQuery filter(Filter filter) {
      hSearchQuery.filter( filter );
      return this;
   }

   /**
    * @return The result size of the query.
    */
   @Override
   public int getResultSize() {
      return hSearchQuery.queryResultSize();
   }

   @Override
   public CacheQuery sort(Sort sort) {
      hSearchQuery.sort( sort );
      return this;
   }

   /**
    * Enable a given filter by its name.
    *
    * @param name of filter.
    * @return a FullTextFilter object.
    */
   @Override
   public FullTextFilter enableFullTextFilter(String name) {
      return hSearchQuery.enableFullTextFilter( name );
   }

   /**
    * Disable a given filter by its name.
    *
    * @param name of filter.
    */
   @Override
   public CacheQuery disableFullTextFilter(String name) {
      hSearchQuery.disableFullTextFilter( name );
      return this;
   }

   /**
    * Sets the the result of the given integer value to the first result.
    *
    * @param firstResult index to be set.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   @Override
   public CacheQuery firstResult(int firstResult) {
      hSearchQuery.firstResult( firstResult );
      return this;
   }
   
   @Override
   public CacheQuery maxResults(int maxResults) {
      hSearchQuery.maxResults( maxResults );
      return this;
   }

   @Override
   public QueryIterator iterator() throws SearchException {
      return iterator(1);
   }

   @Override
   public QueryIterator iterator(int fetchSize) throws SearchException {
      hSearchQuery.getTimeoutManager().start();
      List<EntityInfo> entityInfos = hSearchQuery.queryEntityInfos();
      List<Object> keyList = fromEntityInfosToKeys(entityInfos);
      return new EagerIterator(keyList, cache, fetchSize);
   }

   @Override
   public QueryIterator lazyIterator() {
      return lazyIterator(1);
   }

   @Override
   public QueryIterator lazyIterator(int fetchSize) {
      return new LazyIterator(hSearchQuery, cache, keyTransformationHandler, fetchSize);
   }

   @Override
   public List<Object> list() throws SearchException {
      hSearchQuery.getTimeoutManager().start();
      final List<EntityInfo> entityInfos = hSearchQuery.queryEntityInfos();
      EntityLoader loader = getLoader();
      List<Object> list = loader.load( entityInfos.toArray( new EntityInfo[entityInfos.size()] ) );
      return list;
   }

   private EntityLoader getLoader() {
      return new EntityLoader(cache, keyTransformationHandler);
   }
   
   private List<Object> fromEntityInfosToKeys(final List<EntityInfo> entityInfos) {
      List<Object> keyList = new ArrayList<Object>(entityInfos.size());
      for (EntityInfo ei : entityInfos) {
         Object cacheKey = keyTransformationHandler.stringToKey(ei.getId().toString(), cache.getClassLoader());
         keyList.add(cacheKey);
      }
      return keyList;
   }

   @Override
   public FacetManager getFacetManager() {
      return hSearchQuery.getFacetManager();
   }

   @Override
   public Explanation explain(int documentId) {
      return hSearchQuery.explain(documentId);
   }

   @Override
   public CacheQuery projection(String... fields) {
      hSearchQuery.projection(fields);
      return this;
   }

}
