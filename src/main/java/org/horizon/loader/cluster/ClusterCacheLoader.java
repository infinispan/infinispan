package org.horizon.loader.cluster;

import org.horizon.loader.AbstractCacheLoader;
import org.horizon.loader.CacheLoaderConfig;
import org.horizon.loader.CacheLoaderException;
import org.horizon.Cache;
import org.horizon.container.entries.InternalCacheEntry;
import org.horizon.marshall.Marshaller;

import java.util.Set;

/**
 * // TODO: Mircea: Document this!
 *
 * @author
 */
public class ClusterCacheLoader extends AbstractCacheLoader {

   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   public void start() throws CacheLoaderException {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   public void stop() throws CacheLoaderException {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }
}
