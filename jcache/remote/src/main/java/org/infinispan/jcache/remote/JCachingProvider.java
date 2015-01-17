package org.infinispan.jcache.remote;

import java.net.URI;
import java.util.Properties;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import org.infinispan.jcache.AbstractJCachingProvider;
import org.infinispan.jcache.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Infinispan's SPI hook up to {@link javax.cache.spi.CachingProvider} for the client-server mode.
 */
@MetaInfServices(CachingProvider.class)
@SuppressWarnings("unused")
public class JCachingProvider extends AbstractJCachingProvider {
   private static final Log log = LogFactory.getLog(JCachingProvider.class, Log.class);

   private static final URI DEFAULT_URI = URI.create(JCachingProvider.class.getName());

   @Override
   public URI getDefaultURI() {
      return DEFAULT_URI;
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      return false;
   }

   @Override
   protected CacheManager createCacheManager(ClassLoader classLoader, URI uri, Properties properties) {
      return new JCacheManager(uri, classLoader, this, properties);
   }
}
