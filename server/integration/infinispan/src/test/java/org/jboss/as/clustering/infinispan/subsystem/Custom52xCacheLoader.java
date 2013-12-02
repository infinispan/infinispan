package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;

import java.util.Set;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class Custom52xCacheLoader extends AbstractCacheLoader {
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
   public void start() throws CacheLoaderException {
      
   }

   @Override
   public void stop() throws CacheLoaderException {
      
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return null;  
   }
}
