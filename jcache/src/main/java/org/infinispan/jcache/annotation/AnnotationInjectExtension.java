package org.infinispan.jcache.annotation;

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemoveAll;
import javax.cache.annotation.CacheRemove;
import javax.cache.annotation.CacheResult;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.BeforeBeanDiscovery;
import javax.enterprise.inject.spi.Extension;

/**
 * CDI extension to register additional interceptor bindings
 *
 * @author Galder Zamarreño
 * @author Pete Muir
 * @since 5.3
 */
public class AnnotationInjectExtension implements Extension {

   void registerInterceptorBindings(@Observes BeforeBeanDiscovery event) {
      event.addInterceptorBinding(CacheResult.class);
      event.addInterceptorBinding(CachePut.class);
      event.addInterceptorBinding(CacheRemove.class);
      event.addInterceptorBinding(CacheRemoveAll.class);
   }

}
