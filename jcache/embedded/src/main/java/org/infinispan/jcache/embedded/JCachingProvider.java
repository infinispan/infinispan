package org.infinispan.jcache.embedded;

import java.net.URI;
import java.util.Properties;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

import org.infinispan.jcache.AbstractJCachingProvider;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * Infinispan's SPI hook up to {@link javax.cache.spi.CachingProvider}.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
@MetaInfServices(CachingProvider.class)
@SuppressWarnings("unused")
public class JCachingProvider extends AbstractJCachingProvider {
   private static final Log log = LogFactory.getLog(JCachingProvider.class);

   private static final URI DEFAULT_URI = URI.create(JCachingProvider.class.getName());

   @Override
   public URI getDefaultURI() {
      return DEFAULT_URI;
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      switch (optionalFeature) {
         case STORE_BY_REFERENCE:
            return true;
         default:
            return false;
      }
   }

   @Override
   protected CacheManager createCacheManager(ClassLoader classLoader, URI uri, Properties properties) {
      return new JCacheManager(uri, classLoader, this, properties);
   }
}
