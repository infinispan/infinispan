package org.infinispan.loaders.spi;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An abstract {@link org.infinispan.loaders.spi.CacheLoader} that holds common implementations for some methods
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Navin Surtani
 * @author Tristan Tarrant
 * @since 6.0
 */
public abstract class AbstractCacheLoader implements CacheLoader {
   private static final Log log = LogFactory.getLog(AbstractCacheLoader.class);

   protected volatile StreamingMarshaller marshaller;
   protected volatile Cache<Object, Object> cache;
   protected TimeService timeService;
   protected CacheLoaderConfiguration configuration;

   /**
    * {@inheritDoc} This implementation delegates to {@link CacheLoader#load(Object)}, to ensure that a response is
    * returned only if the entry is not expired.
    */
   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      return load(key) != null;
   }

   @Override
   public void init(CacheLoaderConfiguration config, Cache<?, ?> cache, StreamingMarshaller m) throws
         CacheLoaderException {
      this.marshaller = m;
      if (config == null) throw log.cacheLoaderConfigurationCannotBeNull();
      this.configuration = config;
      this.cache = (Cache<Object, Object>) cache;
      this.timeService = cache.getAdvancedCache().getComponentRegistry().getTimeService();
   }

   final protected <T extends CacheLoaderConfiguration> T validateConfigurationClass(CacheLoaderConfiguration config, Class<T> klass) {
      if (klass.isInstance(config)) {
         return (T) config;
      } else {
         throw log.incompatibleLoaderConfiguration(klass.getName(), config.getClass().getName());
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public CacheLoaderConfiguration getConfiguration() {
      return configuration;
   }
}
