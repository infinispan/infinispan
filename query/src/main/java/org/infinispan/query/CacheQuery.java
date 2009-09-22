/*
 * JBoss, Home of Professional Open Source
 * Copyright ${year}, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Sort;
import org.hibernate.search.FullTextFilter;

import java.util.List;

/**
 * A cache-query is what will be returned when the createQuery() method is run. This object can have methods such
 * as list, setFirstResult,setMaxResults, setFetchSize, getResultSize and setSort.
 *
 * <p/>
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 * @see org.infinispan.query.SearchableCache#createQuery(org.apache.lucene.search.Query, Class[])
 */
public interface CacheQuery extends Iterable
{
   /**
    * Returns the results of a search as a list.
    *
    * @return list of objects that were found from the search.
    */

   List<Object> list();

   /**
    * Returns the results of a search as a {@link QueryIterator} with a given
    * integer parameter - the fetchSize.
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

   QueryIterator iterator();

   /**
    * Lazily loads the results from the Query as a {@link QueryIterator} with a given
    * integer parameter - the fetchSize.
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

   void setFirstResult(int index);

   /**
    * Sets the maximum number of results to the number passed in as a parameter.
    *
    * @param numResults that are to be set to the maxResults.
    */

   void setMaxResults(int numResults);

   /**
    * Gets the integer number of results.
    *
    * @return integer number of results.
    */

   int getResultSize();

   /**
    * Allows lucene to sort the results. Integers are sorted in descending order.
    *
    * @param s - lucene sort object
    */

   void setSort(Sort s);

   /**
    * Enable a given filter by its name.
    *
    * @param name of filter.
    * @return a FullTextFilter object.
    */
   public FullTextFilter enableFullTextFilter(String name);


   /**
    * Disable a given filter by its name.
    *
    * @param name of filter.
    */
   public void disableFullTextFilter(String name);

   /**
    * Allows lucene to filter the results.
    *
    * @param f - lucene filter
    */

   public void setFilter(Filter f);




}
