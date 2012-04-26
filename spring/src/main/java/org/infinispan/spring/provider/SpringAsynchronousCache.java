/**
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
 *   ~
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

package org.infinispan.spring.provider;


import org.infinispan.CacheException;
import org.infinispan.api.BasicCache;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.ExecutionException;

/**
 * <p>
 * A {@link org.springframework.cache.Cache <code>Cache</code>} implementation that delegates to a
 * {@link org.infinispan.Cache <code>org.infinispan.Cache</code>} instance supplied at construction
 * time.
 * </p>
 * <p>
 * The primary difference between this class and {@link org.infinispan.spring.provider.SpringCache}
 * is that the {@link #put(Object, Object)}, {@link #evict(Object)}, and {@link #clear()} are
 * performed using the asynchronous operation of the {@link org.infinispan.Cache <code>org.infinispan.Cache</code>}
 * </p>
 *
 * @author <a href="mailto:ryebrye AT gmail DOT com">Ryan Gardner</a>
 *
 */
public class SpringAsynchronousCache extends SpringCache {

    /**
     * @param nativeCache
     */
    public SpringAsynchronousCache(final BasicCache<Object, Object> nativeCache) {
        super(nativeCache);
    }

    @Override
    public ValueWrapper get(final Object key) {
      // It may be better to just do the get synchronously.
      // (the contract of the spring cache requires you to return null if the key isn't in the cache, or a ValueWrapper if it is)
      return (nativeCache.containsKey(key)) ? (new NotifyingFutureValueWrapper(nativeCache.getAsync(key))) : null;
    }

    /**
     * Associate the specified value with the specified key in this cache.
     * <p>If the cache previously contained a mapping for this key, the old
     * value is replaced by the specified value.
     *
     * @param key   the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     */
    @Override
    public void put(Object key, Object value) {
        nativeCache.putAsync(key, value);
    }

    /**
     * Evict the mapping for this key from this cache if it is present.
     *
     * @param key the key whose mapping is to be removed from the cache
     */
    @Override
    public void evict(Object key) {
        nativeCache.removeAsync(key);
    }

    /**
     * Remove all mappings from the cache.
     */
    @Override
    public void clear() {
        nativeCache.clearAsync();
    }

    public static final class NotifyingFutureValueWrapper implements ValueWrapper {
        private NotifyingFuture<Object> futureObject;

        public NotifyingFutureValueWrapper(NotifyingFuture<Object> futureObject) {
            this.futureObject = futureObject;
        }

        /**
         * Return the actual value in the cache.
         *
         * If there is an exception when retrieving the value, a null value will be returned
         * which Spring will take to indicate that there is no entry in the cache.
         *
         * (This prevents an exception from bubbling up to something in Spring that uses @Cacheable)
         */
        @Override
        public Object get() {
            try {
                return futureObject.get();
            } catch (CacheException e) {
                return null;
            } catch (InterruptedException e) {
                return null;
            } catch (ExecutionException e) {
                return null;
            }
        }
    }
}
