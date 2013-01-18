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
package org.infinispan.jsr107.cache;

import java.util.Collections;
import java.util.Set;

import javax.cache.Cache.Entry;
import javax.cache.CacheLoader;

import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;

public class InfinispanCacheLoaderAdapter<K, V> extends AbstractCacheLoader {

   private final CacheLoader<K, V> delegate;

   public InfinispanCacheLoaderAdapter(CacheLoader<K, V> delegate) {
      super();
      this.delegate = delegate;
   }

   @SuppressWarnings("unchecked")
   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      Entry<K, V> e = delegate.load((K) key);
      // TODO or whatever type of entry is more appropriate?
      return new ImmortalCacheEntry(e.getKey(), e.getValue());
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return Collections.emptySet();
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return Collections.emptySet();
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return Collections.emptySet();
   }

   @Override
   public void start() throws CacheLoaderException {
      // TODO
   }

   @Override
   public void stop() throws CacheLoaderException {
      // TODO
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return null;
   }

}
