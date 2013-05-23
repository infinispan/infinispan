/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A factory for {@link InternalCacheEntry} and {@link InternalCacheValue} instances.
 *
 * @author Manik Surtani
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface InternalEntryFactory {

   /**
    * Creates a new {@link InternalCacheEntry} instance based on the key, value, version and timestamp/lifespan
    * information reflected in the {@link CacheEntry} instance passed in.
    * @param cacheEntry cache entry to copy
    * @return a new InternalCacheEntry
    */
   InternalCacheEntry create(CacheEntry cacheEntry);

   /**
    * Creates a new {@link InternalCacheEntry} instance based on the version and timestamp/lifespan
    * information reflected in the {@link CacheEntry} instance passed in.  Key and value are both passed in 
    * explicitly.
    * @param key key to use
    * @param value value to use
    * @param cacheEntry cache entry to retrieve version and timestamp/lifespan information from
    * @return a new InternalCacheEntry
    */
   InternalCacheEntry create(Object key, Object value, InternalCacheEntry cacheEntry);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use           
    * @param metadata metadata for entry
    * @return a new InternalCacheEntry
    */
   InternalCacheEntry create(Object key, Object value, Metadata metadata);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use
    * @param metadata metadata for entry
    * @param lifespan lifespan to use
    * @param maxIdle maxIdle to use
    * @return a new InternalCacheEntry
    */
   InternalCacheEntry create(Object key, Object value, Metadata metadata, long lifespan, long maxIdle);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use
    * @param metadata metadata for entry
    * @param created creation timestamp to use
    * @param lifespan lifespan to use
    * @param lastUsed lastUsed timestamp to use
    * @param maxIdle maxIdle to use
    * @return a new InternalCacheEntry
    */
   InternalCacheEntry create(Object key, Object value, Metadata metadata, long created, long lifespan, long lastUsed, long maxIdle);

   /**
    * Creates a new {@link InternalCacheEntry} instance
    * @param key key to use
    * @param value value to use
    * @param version version to use
    * @param created creation timestamp to use
    * @param lifespan lifespan to use
    * @param lastUsed lastUsed timestamp to use
    * @param maxIdle maxIdle to use
    * @return a new InternalCacheEntry
    */
   // To be deprecated, once metadata object can be retrieved remotely...
   InternalCacheEntry create(Object key, Object value, EntryVersion version, long created, long lifespan, long lastUsed, long maxIdle);

   /**
    * TODO: Adjust javadoc
    *
    * Updates an existing {@link InternalCacheEntry} with new metadata.  This may result in a new
    * {@link InternalCacheEntry} instance being created, as a different {@link InternalCacheEntry} implementation
    * may be more appropriate to suit the new metadata values.  As such, one should consider the {@link InternalCacheEntry}
    * passed in as a parameter as passed by value and not by reference.
    * 
    * @param cacheEntry original internal cache entry
    * @param metadata new metadata
    * @return a new InternalCacheEntry instance
    */
   InternalCacheEntry update(InternalCacheEntry cacheEntry, Metadata metadata);

   /**
    * Creates an {@link InternalCacheValue} based on the {@link InternalCacheEntry} passed in.
    * 
    * @param cacheEntry to use to generate a {@link InternalCacheValue}
    * @return an {@link InternalCacheValue}
    */
   InternalCacheValue createValue(CacheEntry cacheEntry);
}
