package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class Custom52xCacheStore extends AbstractCacheStore {
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      
   }

   @Override
   public void store(InternalCacheEntry internalCacheEntry) throws CacheLoaderException {
      
   }

   @Override
   public void fromStream(ObjectInput objectInput) throws CacheLoaderException {
      
   }

   @Override
   public void toStream(ObjectOutput objectOutput) throws CacheLoaderException {
      
   }

   @Override
   public void clear() throws CacheLoaderException {
      
   }

   @Override
   public boolean remove(Object o) throws CacheLoaderException {
      return false;  
   }

   @Override
   public InternalCacheEntry load(Object o) throws CacheLoaderException {
      return null;  
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return null;  
   }

   @Override
   public Set<InternalCacheEntry> load(int i) throws CacheLoaderException {
      return null;  
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> objects) throws CacheLoaderException {
      return null;  
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return null;  
   }
}
