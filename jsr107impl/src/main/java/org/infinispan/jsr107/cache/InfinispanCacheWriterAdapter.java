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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import javax.cache.CacheWriter;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;

public class InfinispanCacheWriterAdapter<K, V> extends AbstractCacheStore {

   private final CacheWriter<K, V> delegate;

   public InfinispanCacheWriterAdapter(CacheWriter<K, V> delegate) {
      super();
      this.delegate = delegate;
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      delegate.write(new InfinispanCacheEntry(entry));
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      // TODO
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      // TODO
   }

   @Override
   public void clear() throws CacheLoaderException {
      // TODO
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      // TODO
      delegate.delete(key);
      return false;
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {      
      //TODO
      return null;
      
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      // TODO
      return Collections.emptySet();
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      // TODO
      return Collections.emptySet();
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      // TODO
      return Collections.emptySet();
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      // TODO
      return null;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      // TODO
   }
}
