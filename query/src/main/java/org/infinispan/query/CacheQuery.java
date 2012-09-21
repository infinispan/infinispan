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
package org.infinispan.query;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.hibernate.search.FullTextFilter;
import org.hibernate.search.query.engine.spi.FacetManager;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A cache-query is what will be returned when the getQuery() method is run on {@link SearchManagerImpl}. This object can
 * have methods such as list, setFirstResult,setMaxResults, setFetchSize, getResultSize and setSort.
 * <p/>
 *
 * @author Manik Surtani
 * @author Navin Surtani
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @see SearchManagerImpl#getQuery(org.apache.lucene.search.Query)
 */
public interface CacheQuery extends Iterable<Object> {

   /**
    * Returns the results of a search as a list.
    *
    * @return list of objects that were found from the search.
    */
   List<Object> list();

   /**
    * Returns the results of a search as a {@link QueryIterator} with a given integer parameter - the fetchSize.
    *
    * @param fetchSize integer to be given to the implementation constructor.
    * @return a QueryResultIterator which can be used to iterate through the results that were found.
    */
   QueryIterator iterator(int fetchSize);

   /**
    * Returns the results of a search as a {@link QueryIterator}. This calls {@link CacheQuery#iterator(int fetchSize)}
    * but uses a default fetchSize of 1.
    *
    * @return a QueryResultIterator which can be used to iterate through the results that were found.
    */
   @Override
   QueryIterator iterator();

   /**
    * Lazily loads the results from the Query as a {@link QueryIterator} with a given integer parameter - the
    * fetchSize.
    *
    * @param fetchSize integer to be passed into the lazy implementation of {@link QueryIterator}
    * @return a QueryResultIterator which can be used to <B>lazily</B> iterate through results.
    */
   QueryIterator lazyIterator(int fetchSize);

   /**
    * Calls the {@link CacheQuery#lazyIterator(int fetchSize)} method but passes in a default 1 as a parameter.
    *
    * @return a QueryResultIterator which can be used to <B>lazily</B> iterate through results.
    */
   QueryIterator lazyIterator();

   /**
    * Sets a result with a given index to the first result.
    *
    * @param index of result to be set to the first.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   CacheQuery firstResult(int index);

   /**
    * Sets the maximum number of results to the number passed in as a parameter.
    *
    * @param numResults that are to be set to the maxResults.
    */
   CacheQuery maxResults(int numResults);

   /**
    * @return return the manager for all faceting related operations
    */
   FacetManager getFacetManager();

   /**
    * Gets the integer number of results.
    *
    * @return integer number of results.
    */
   int getResultSize();

   /**
    * Return the Lucene {@link org.apache.lucene.search.Explanation}
    * object describing the score computation for the matching object/document
    * in the current query
    *
    * @param documentId Lucene Document id to be explain. This is NOT the object key
    * @return Lucene Explanation
    */
   Explanation explain(int documentId);

   /**
    * Allows lucene to sort the results. Integers are sorted in descending order.
    *
    * @param s - lucene sort object
    */
   CacheQuery sort(Sort s);

   /**
    * Defines the Lucene field names projected and returned in a query result
    * Each field is converted back to it's object representation, an Object[] being returned for each "row"
    * <p/>
    * A projectable field must be stored in the Lucene index and use a {@link org.hibernate.search.bridge.TwoWayFieldBridge}
    * Unless notified in their JavaDoc, all built-in bridges are two-way. All @DocumentId fields are projectable by design.
    * <p/>
    * If the projected field is not a projectable field, null is returned in the object[]
    *
    * @param fields the projected field names
    * @return {@code this}  to allow for method chaining
    */
   CacheQuery projection(String... fields);

   /**
    * Enable a given filter by its name.
    *
    * @param name of filter.
    * @return a FullTextFilter object.
    */
   FullTextFilter enableFullTextFilter(String name);

   /**
    * Disable a given filter by its name.
    *
    * @param name of filter.
    */
   CacheQuery disableFullTextFilter(String name);

   /**
    * Allows lucene to filter the results.
    *
    * @param f - lucene filter
    */
   CacheQuery filter(Filter f);

   /**
    * Set the timeout for this query. If the query hasn't finished processing before the timeout,
    * an exception will be thrown.
    *
    * @param timeout the timeout duration
    * @param timeUnit the time unit of the timeout parameter
    * @return
    */
   CacheQuery timeout(long timeout, TimeUnit timeUnit);
}
