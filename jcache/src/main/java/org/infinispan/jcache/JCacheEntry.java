/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
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
package org.infinispan.jcache;

import javax.cache.Cache.Entry;

/**
 * Infinispan implementation of {@link javax.cache.Cache.Entry<K, V>}.
 *
 * @param <K> the type of key maintained by this cache entry
 * @param <V> the type of value maintained by this cache entry
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class JCacheEntry<K, V> implements Entry<K, V> {

   protected final K key;

   protected final V value;

   public JCacheEntry(K key, V value) {
      this.key = key;
      this.value = value;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      return value;
   }

}
