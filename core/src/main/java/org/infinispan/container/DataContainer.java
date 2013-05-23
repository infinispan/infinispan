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
package org.infinispan.container;

import java.util.Collection;
import java.util.Set;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * The main internal data structure which stores entries
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DataContainer extends Iterable<InternalCacheEntry> {

   /**
    * Retrieves a cached entry
    * @param k key under which entry is stored
    * @return entry, if it exists and has not expired, or null if not
    */
   InternalCacheEntry get(Object k);
   
   /**
    * Retrieves a cache entry in the same way as {@link #get(Object)}}
    * except that it does not update or reorder any of the internal constructs. 
    * I.e., expiration does not happen, and in the case of the LRU container, 
    * the entry is not moved to the end of the chain.
    * 
    * This method should be used instead of {@link #get(Object)}} when called
    * while iterating through the data container using methods like {@link #keySet()} 
    * to avoid changing the underlying collection's order.
    * 
    * @param k key under which entry is stored
    * @return entry, if it exists, or null if not
    */
   InternalCacheEntry peek(Object k);

   /**
    * Puts an entry in the cache along with metadata adding information such
    * lifespan of entry, max idle time, version information...etc.
    *
    * @param k key under which to store entry
    * @param v value to store
    * @param metadata metadata of the entry
    */
   void put(Object k, Object v, Metadata metadata);

   /**
    * Tests whether an entry exists in the container
    * @param k key to test
    * @return true if entry exists and has not expired; false otherwise
    */
   boolean containsKey(Object k);

   /**
    * Removes an entry from the cache
    * @param k key to remove
    * @return entry removed, or null if it didn't exist or had expired
    */
   InternalCacheEntry remove(Object k);

   /**
    *
    * @return count of the number of entries in the container
    */
   int size();

   /**
    * Removes all entries in the container
    */
   @Stop(priority = 999)
   void clear();

   /**
    * Returns a set of keys in the container. When iterating through the container using this method,
    * clients should never call {@link #get()} method but instead {@link #peek()}, in order to avoid
    * changing the order of the underlying collection as a side of effect of iterating through it.
    * 
    * @return a set of keys
    */
   Set<Object> keySet();

   /**
    * @return a set of values contained in the container
    */
   Collection<Object> values();

   /**
    * Returns a mutable set of immutable cache entries exposed as immutable Map.Entry instances. Clients 
    * of this method such as Cache.entrySet() operation implementors are free to convert the set into an 
    * immutable set if needed, which is the most common use case. 
    * 
    * If a client needs to iterate through a mutable set of mutable cache entries, it should iterate the 
    * container itself rather than iterating through the return of entrySet().
    * 
    * @return a set of immutable cache entries
    */
   Set<InternalCacheEntry> entrySet();

   /**
    * Purges entries that have passed their expiry time
    */
   void purgeExpired();
}
