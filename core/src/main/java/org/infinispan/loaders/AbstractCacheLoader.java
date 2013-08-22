package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.marshall.LegacyMarshallerAdapter;
import org.infinispan.marshall.LegacyStreamingMarshallerAdapter;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.TimeService;

/**
 * An abstract {@link org.infinispan.loaders.CacheLoader} that holds common implementations for some methods
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Deprecated
public abstract class AbstractCacheLoader implements CacheLoader {

   protected volatile StreamingMarshaller marshaller;
   protected volatile Cache<Object, Object> cache;
   protected TimeService timeService;

   /**
    * {@inheritDoc} This implementation delegates to {@link CacheLoader#load(Object)}, to ensure that a response is
    * returned only if the entry is not expired.
    */
   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      return load(key) != null;
   }

   @Override
   public void init(CacheLoaderConfig config, Cache cache, StreamingMarshaller m) throws CacheLoaderException {
      this.marshaller = m;
      if (config == null) throw new IllegalStateException("Null config!!!");
      this.cache = cache;
      this.timeService = cache.getAdvancedCache().getComponentRegistry().getTimeService();
   }
}
