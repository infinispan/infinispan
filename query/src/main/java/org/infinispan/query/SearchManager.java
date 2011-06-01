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

/**
 * The SearchManager is the entry point to create full text queries on top of a cache.
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public interface SearchManager {

   /**
    * This is a simple method that will just return a {@link CacheQuery}, filtered according to a set of classes passed
    * in.  If no classes are passed in, it is assumed that no type filtering is performed and so all known types will
    * be searched.
    *
    * @param luceneQuery - {@link org.apache.lucene.search.Query}
    * @param classes - optionally only return results of type that matches this list of acceptable types
    * @return the CacheQuery object which can be used to iterate through results
    */
   public CacheQuery getQuery(Query luceneQuery, Class<?>... classes);

   /**
    * Experimental.
    * Provides Hibernate Search DSL to build full text queries
    * @return 
    */
   public EntityContext buildQueryBuilderForClass(Class<?> entityType);

   /**
    * Experimental.
    * Access the SearchFactory
    */
   public SearchFactory getSearchFactory();

   /**
    * 
    * This should be hided into getQuery method...
    * 
    * @param luceneQuery
    * @param classes
    * @return
    */
   CacheQuery getClusteredQuery(Query luceneQuery, Class<?>... classes);

}
