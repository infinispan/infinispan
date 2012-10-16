/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter.session;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

public interface Session {

   /**
    * Obtains the CacheManager to which this session is attached
    */
   EmbeddedCacheManager getCacheManager();

   /**
    * Obtains the currently selected cache. If none has been selected, the default cache is returned
    */
   <K, V> Cache<K, V> getCurrentCache();

   /**
    * Returns the name of the currently selected cache. If none has been selected, the default cache is returned
    */
   String getCurrentCacheName();

   /**
    * Returns a named cache. If the cacheName parameter is null, the current cache is returned
    *
    * @param cacheName
    * @return the cache identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName);

   /**
    * Sets the current cache.
    *
    * @param cacheName
    */
   void setCurrentCache(String cacheName);

   /**
    * Creates a new cache
    *
    * @param cacheName the name of the new cache
    * @param baseCacheName the existing named cache to use for defaults
    */
   void createCache(String cacheName, String baseCacheName);

   /**
    * Resets the session, by aborting any dangling batches and transactions and updating the timestamp
    */
   void reset();

   /**
    * Returns the time the session was last accessed
    *
    * @return
    */
   long getTimestamp();

   /**
    * Returns the unique id of this session
    *
    * @return
    */
   String getId();
}
