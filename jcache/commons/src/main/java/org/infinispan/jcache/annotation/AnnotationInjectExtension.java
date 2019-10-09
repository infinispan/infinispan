package org.infinispan.jcache.annotation;

import java.lang.annotation.Annotation;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.logging.BasicLogFactory;
import org.infinispan.commons.util.Util;
import org.jboss.logging.BasicLogger;
import org.kohsuke.MetaInfServices;

/**
 * CDI extension to register additional interceptor bindings
 *
 * @author Galder Zamarreño
 * @author Pete Muir
 * @since 5.3
 */
@MetaInfServices
public class AnnotationInjectExtension implements Extension {

   private static final BasicLogger log = BasicLogFactory.getLog(AnnotationInjectExtension.class);

   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
      try {
         event.addInterceptorBinding(Util.<Annotation>loadClass("javax.cache.annotation.CacheResult", null));
         event.addInterceptorBinding(Util.<Annotation>loadClass("javax.cache.annotation.CachePut", null));
         event.addInterceptorBinding(Util.<Annotation>loadClass("javax.cache.annotation.CacheRemove", null));
         event.addInterceptorBinding(Util.<Annotation>loadClass("javax.cache.annotation.CacheRemoveAll", null));
      } catch (CacheConfigurationException ex) {
         log.debug("Cache API not present on class path: " + ex.getMessage());
      }
   }

}
