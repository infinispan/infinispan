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

import org.springframework.cache.Cache;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.Cache <code>Cache</code>} implementation that delegates to a
 * {@link org.infinispan.Cache <code>org.infinispan.Cache</code>} instance supplied at construction
 * time.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
public class SpringCache<K, V> implements Cache<K, V> {

   private final org.infinispan.Cache<K, V> nativeCache;

   /**
    * @param nativeCache
    */
   public SpringCache(final org.infinispan.Cache<K, V> nativeCache) {
      Assert.notNull(nativeCache, "A non-null Infinispan cache implementation is required");
      this.nativeCache = nativeCache;
   }

   /**
    * @see org.springframework.cache.Cache#getName()
    */
   @Override
   public String getName() {
      return this.nativeCache.getName();
   }

   /**
    * @see org.springframework.cache.Cache#getNativeCache()
    */
   @Override
   public org.infinispan.Cache<K, V> getNativeCache() {
      return this.nativeCache;
   }

   /**
    * @see org.springframework.cache.Cache#containsKey(java.lang.Object)
    */
   @Override
   public boolean containsKey(final Object key) {
      return this.nativeCache.containsKey(key);
   }

   /**
    * @see org.springframework.cache.Cache#get(java.lang.Object)
    */
   @Override
   public V get(final Object key) {
      return this.nativeCache.get(key);
   }

   /**
    * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
    */
   @Override
   public V put(final K key, final V value) {
      return this.nativeCache.put(key, value);
   }

   /**
    * @see org.springframework.cache.Cache#putIfAbsent(java.lang.Object, java.lang.Object)
    */
   @Override
   public V putIfAbsent(final K key, final V value) {
      return this.nativeCache.putIfAbsent(key, value);
   }

   /**
    * @see org.springframework.cache.Cache#remove(java.lang.Object)
    */
   @Override
   public V remove(final Object key) {
      return this.nativeCache.remove(key);
   }

   /**
    * @see org.springframework.cache.Cache#remove(java.lang.Object, java.lang.Object)
    */
   @Override
   public boolean remove(final Object key, final Object value) {
      return this.nativeCache.remove(key, value);
   }

   /**
    * @see org.springframework.cache.Cache#replace(java.lang.Object, java.lang.Object,
    *      java.lang.Object)
    */
   @Override
   public boolean replace(final K key, final V oldValue, final V newValue) {
      return this.nativeCache.replace(key, oldValue, newValue);
   }

   /**
    * @see org.springframework.cache.Cache#replace(java.lang.Object, java.lang.Object)
    */
   @Override
   public V replace(final K key, final V value) {
      return this.nativeCache.replace(key, value);
   }

   /**
    * @see org.springframework.cache.Cache#clear()
    */
   @Override
   public void clear() {
      this.nativeCache.clear();
   }

   /**
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString() {
      return "InfinispanCache [nativeCache = " + this.nativeCache + "]";
   }

}
